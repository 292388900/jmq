package com.ipd.jmq.replication;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.network.*;
import com.ipd.jmq.common.network.v3.codec.JMQCodec;
import com.ipd.jmq.common.network.v3.command.*;
import com.ipd.jmq.common.network.v3.netty.NettyDecoder;
import com.ipd.jmq.common.network.v3.netty.NettyEncoder;
import com.ipd.jmq.server.store.Store;
import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.common.network.netty.AbstractServer;
import com.ipd.jmq.toolkit.buffer.RByteBuffer;
import com.ipd.jmq.toolkit.concurrent.EventBus;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.time.MicroPeriod;
import com.ipd.jmq.toolkit.time.SystemClock;
import io.netty.channel.ChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ipd.jmq.common.network.v3.command.CommandUtils.createBooleanAck;
import static com.ipd.jmq.common.network.v3.command.CommandUtils.createResponse;

/**
 * Created by guoliang5 on 2016/8/16.
 */
public class ReplicationMasterService extends Service implements ReplicationMaster {
    public ReplicationMasterService(Broker master, ReplicationConfig  config,
                                    ReplicationListener replicationListener, Store store) {
        if (master == null) {
            throw new IllegalArgumentException("master can not be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("config can not be null");
        }
        if (store == null) {
            throw new IllegalArgumentException("store can not be null");
        }
        if (config.getServerConfig() == null) {
            throw new IllegalArgumentException("config.getServerConfig can not be null");
        }
        this.replicationListener = replicationListener;
        this.config = config;
        this.master = master;
        this.store = store;
    }

    @Override
    public long getWaterMark() {
        return waterMark;
    }

    @Override
    public boolean isInsync() {
        return isInsync;
    }

    @Override
    public List<Replica> getAllReplicas() {
        List<Replica> result;
        synchronized (replicaList) {
            result = new ArrayList<Replica>(replicaList);
        }
        return result;
    }

    @Override
    public List<Replica> getInsyncReplicas() {
        List<Replica> result = new ArrayList<Replica>(replicaList.size());
        long maxOffset = store.getMaxOffset();
        synchronized (replicaList) {
            for (ReplicaImpl replica : replicaList) {
                if (maxOffset - replica.getReplicatedOffset() < config.getInsyncSize()) {
                    result.add(replica);
                }
            }
        }
        return result;
    }

    @Override
    public void addListener(EventListener<ReplicaEvent> listener) {
        eventBus.addListener(listener);
    }

    @Override
    public void removeListener(EventListener<ReplicaEvent> listener) {
        eventBus.removeListener(listener);
    }

    @Override
    public RepStat getRepStat() {
        return repStat;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        nettyServer = new ReplicationMasterServer(config.getServerConfig());
        nettyServer.addListener(new EventListener<TransportEvent>() {
            @Override
            public void onEvent(TransportEvent transportEvent) {
                if (transportEvent.getType() == TransportEvent.EventType.CLOSE || transportEvent.getType() == TransportEvent.EventType.EXCEPTION) {
                    ReplicaImpl replica = null;
                    synchronized (replicaList) {
                        for (int i = 0; i < replicaList.size(); ++i) {
                            if (replicaList.get(i).getTransport().equals(transportEvent.getTransport())) {
                                replica = replicaList.get(i);
                                replicaList.remove(i);
                                break;
                            }
                        }
                    }
                    if (replica != null) {
                        replica.close();
                        eventBus.inform(new ReplicaEvent(ReplicaEvent.State.esRemoveReplica, 0, replica));
                        logger.info(String.format("%s was disconnected", replica.getBroker().getName()));
                        if (replicationListener != null) {
                            replicationListener.onRemoveReplica(replica);
                        }
                    }
                }
            }
        });
        nettyServer.setFactory(new ReplicationCommandHandlerFactory());
        nettyServer.start();
        if (replicationListener != null) {
            replicationListener.onStart();
        }
    }

    @Override
    protected void doStop() {
        if (replicationListener != null) {
            replicationListener.onStop();
        }
        super.doStop();
        nettyServer.stop();
        nettyServer = null;
        logger.info("ReplicationMasterService is stopped");
    }

    private class ReplicationMasterServer extends AbstractServer {
        public ReplicationMasterServer(ServerConfig config) {
            super(config);
        }

        @Override
        protected ChannelHandler[] createChannelHandlers() {
            JMQCodec codec = new JMQCodec();
            return new ChannelHandler[]{new NettyDecoder(codec), new NettyEncoder(codec), commandInvocation};
        }
    }

    private class ReplicationCommandHandlerFactory implements CommandHandlerFactory {
        @Override
        public CommandHandler getHandler(Command command) {
            JMQPayload payload = (JMQPayload) command.getPayload();
            if (payload == null) return null;
            switch (payload.type()) {
                case CmdTypes.IDENTITY      :
                    return identityHandler;
                case CmdTypes.GET_JOURNAL   :
                    return getJournalHandler;
                default:
                    return null;
            }
        }

        private CommandHandler identityHandler = new CommandHandler() {
            @Override
            public Command process(Transport transport, Command command) throws TransportException {
                if (!isStarted()) {
                    //状态设置为服务不可用
                    return createBooleanAck(command.getHeader().getRequestId(), JMQCode.CN_SERVICE_NOT_AVAILABLE);
                }

                // 身份信息
                Identity identity = (Identity) command.getPayload();
                // 获取绑定的复制会话
                ReplicaImpl replica = (ReplicaImpl) transport.attr(IDENTITY);
                if (replica == null) {
                    replica = new ReplicaImpl(identity.getBroker(), transport, Replica.State.Online);
                    transport.attr(IDENTITY, replica);
                    synchronized (replicaList) {
                        replicaList.add(replica);
                    }
                    eventBus.inform(new ReplicaEvent(ReplicaEvent.State.esAddReplica, 0, replica));
                    logger.info(String.format("%s was connected", identity.getBroker().getName()));
                    if (replicationListener != null) {
                        replicationListener.onAddReplica(replica);
                    }
                }

                // 状态设置为成功
                return createBooleanAck(command.getHeader().getRequestId(), JMQCode.SUCCESS);
            }
        };

        private CommandHandler getJournalHandler = new CommandHandler() {
            @Override
            public Command process(Transport transport, Command command) throws TransportException {
                final ReplicaImpl replica = (ReplicaImpl) transport.attr(IDENTITY);
                if (replica == null) {
                    logger.error("replica {} has not identity!", transport.remoteAddress());
                    return createBooleanAck(command.getHeader().getRequestId(), JMQCode
                            .CN_SERVICE_NOT_AVAILABLE);
                }
                GetJournal getJournal = (GetJournal) command.getPayload();
                //得到slave请求数据的起始位置
                long offset = getJournal.getOffset();
                //得到slave请求数据的最大位置
                long maxOffset = store.getMaxOffset();

                updateWaterMark(replica, offset, maxOffset);

                // 如果没有可读数据并且配置了SlaveLongPullDuration，则以LongPull模式处理
                if (offset == maxOffset && getJournal.getPullTimeout() > 0) {
                    processLongPull(transport, command);
                    return null;
                }

                //构造应答命令
                GetJournalAck getJournalAck = new GetJournalAck();
                Command response = createResponse(command, getJournalAck);
                RByteBuffer refBuf = null;
                MicroPeriod period = new MicroPeriod();
                period.begin();
                try {
                    long minOffset = store.getMinOffset();
                    if (offset < minOffset) {
                        offset = minOffset;
                    } else if (offset > maxOffset) {
                        offset = maxOffset;
                    }
                    //获取数据,会根据slave和master的偏移位置、master已经消费过的位置以及数据文件大小等算出每次复制的数据块大小
                    refBuf = store.readMessageBlock(offset, config.getBlockSize());

                    if (refBuf == null || refBuf.getBuffer() == null || refBuf.getBuffer().remaining() == 0) {
                        logger.warn("replica cannot read message block: offset={}, maxOffset={}", offset, maxOffset);
                        if (refBuf != null) {
                            refBuf.release();
                        }
                        offset = store.getMinOffset();
                    }
                    getJournalAck.setBuffer(refBuf);
                    getJournalAck.setOffset(offset);
                    getJournalAck.setInsync((offset >= waterMark) && (maxOffset - waterMark <= config.getInsyncSize()));
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("GetJournalAck offset:%d, maxOffset:%d, size:%d", offset, maxOffset,
                                getJournalAck.getBuffer() == null ? 0 : getJournalAck.getBuffer().remaining()));
                    }
                } catch (Throwable e) {
                    if (refBuf != null) {
                        refBuf.release();
                    }
                    logger.error("replica get journal error offset: " + offset, e);
                    repStat.error();
                    return createBooleanAck(command.getHeader().getRequestId(), JMQCode.SE_IO_ERROR);
                }

                if (response !=null){
                    try{
                        transport.acknowledge(command, response, null);
                        period.end();
                        repStat.success(1, refBuf == null ? 0 : refBuf.remaining(), (int) period.time());
                    }catch (Exception e){
                        if (refBuf != null) {
                            refBuf.release();
                        }
                        repStat.error();
                        logger.error("replica get journal error offset: " + offset, e);
                    }
                }

                return null;
            }

            private void processLongPull(final Transport transport, final Command request) {
                // 如果pullTimeout>=2秒则返减1秒的值为超时，否则返回超时时间的一半
                final GetJournal getJournal = (GetJournal) request.getPayload();
                int pullTimeout = (getJournal.getPullTimeout() >= 2000 ? getJournal.getPullTimeout() - 1000 : getJournal.getPullTimeout() / 2);
                final long endTime = SystemClock.now() + pullTimeout;
                scheduledExecutorService.schedule(new Runnable() {
                    @Override
                    public void run() {
                        long maxOffset = store.getMaxOffset();
                        try {
                            if (SystemClock.now() >= endTime) {
                                // 超时，返回空数据
                                RByteBuffer emptyBuf = new RByteBuffer(ByteBuffer.allocate(0), null);
                                transport.acknowledge(request, createResponse(request, new GetJournalAck().offset(getJournal.getOffset()).buffer(emptyBuf)), null);
                                return;
                            }
                            if (getJournal.getOffset() == maxOffset) {
                                scheduledExecutorService.schedule(this, ReplicationConfig.SLAVE_LONGPULL_INTERVAL, TimeUnit.MILLISECONDS);
                            } else {
                                // 有数据，则再次调用process()获取数据
                                getJournal.setMaxOffset(maxOffset);
                                Command response = process(transport, request);
                                transport.acknowledge(request, response, null);
                            }
                        } catch (TransportException e) {
                            if (retryCount == ReplicationConfig.MAX_RETRY_COUNT)
                                logger.error("processLongPull error: offset={}, maxOffset={}", getJournal.getOffset(), maxOffset, e);
                            if (--retryCount > 0) {
                                scheduledExecutorService.schedule(this, ReplicationConfig.SLAVE_LONGPULL_INTERVAL, TimeUnit.MILLISECONDS);
                            } else {
                                logger.warn("processLongPull retry {} times always error:", ReplicationConfig.MAX_RETRY_COUNT, e);
                            }
                        }
                    }
                    int retryCount = ReplicationConfig.MAX_RETRY_COUNT;
                }, ReplicationConfig.SLAVE_LONGPULL_INTERVAL, TimeUnit.MILLISECONDS);
            }
        };
    }

    private void updateWaterMark(ReplicaImpl replica, long offset, long maxOffset) {
        if (offset > replica.getReplicatedOffset()) {
            replica.setReplicatedOffset(offset);
            ReplicaEvent.State state = ReplicaEvent.State.esInsync;
            ReplicaImpl insyncReplica;
            synchronized (replicaList) {
                Collections.sort(replicaList);
                insyncReplica = replicaList.size() < config.getInsyncCount() ? null : replicaList.get(replicaList.size() - config.getInsyncCount());
            }
            if (insyncReplica == null || (maxOffset - insyncReplica.getReplicatedOffset() > config.getInsyncSize())) {
                state = ReplicaEvent.State.esSynchronizing;
            }
            isInsync = (state == ReplicaEvent.State.esInsync);
            if (insyncReplica != null) {
                waterMark = insyncReplica.getReplicatedOffset();
                eventBus.inform(new ReplicaEvent(state, waterMark, insyncReplica));
            }
        }
    }

    public static final Logger logger = LoggerFactory.getLogger(ReplicationMasterService.class);

    private static final String IDENTITY = "identity";
    private volatile long waterMark = 0;
    private volatile boolean isInsync = false;
    private Broker master;
    private RepStat repStat = new RepStat();
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private List<ReplicaImpl> replicaList = new ArrayList<ReplicaImpl>();
    private EventBus<ReplicaEvent> eventBus = new EventBus<ReplicaEvent>("ReplicationMaster.Event");
    private ReplicationListener replicationListener;
    private ReplicationConfig config;
    private Store store;
    private ReplicationMasterServer nettyServer;
}
