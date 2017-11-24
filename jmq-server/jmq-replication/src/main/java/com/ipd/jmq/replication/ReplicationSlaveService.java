package com.ipd.jmq.replication;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.network.*;
import com.ipd.jmq.common.network.v3.command.GetJournal;
import com.ipd.jmq.common.network.v3.command.GetJournalAck;
import com.ipd.jmq.common.network.v3.command.Identity;
import com.ipd.jmq.common.network.v3.command.JMQHeader;
import com.ipd.jmq.common.network.v3.netty.NettyClient;
import com.ipd.jmq.server.store.Store;
import com.ipd.jmq.server.store.StoreUnsafe;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.common.network.v3.command.CommandCallback;
import com.ipd.jmq.common.network.v3.command.Direction;
import com.ipd.jmq.toolkit.buffer.RByteBuffer;
import com.ipd.jmq.toolkit.concurrent.EventBus;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.concurrent.NamedThreadFactory;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static com.ipd.jmq.common.network.v3.command.CmdTypes.*;
import static com.ipd.jmq.replication.ReplicaEvent.*;

/**
 * 从节点复制服务
 * TODO:1.reset syncMode,2.calculate getJournal speed ,3.limit send message speed if on increment replication
 *
 * @author tianya
 * @since 2014-04-24
 */
public class ReplicationSlaveService extends Service {
    public ReplicationSlaveService(Broker master, Broker slave, ReplicationConfig config,
                                   ReplicationListener replicationListener, Store store, StoreUnsafe storeUnsafe) {
        if (master == null) {
            throw new IllegalArgumentException("master can not be null");
        }
        if (slave == null) {
            throw new IllegalArgumentException("slave can not be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("config can not be null");
        }
        if (store == null) {
            throw new IllegalArgumentException("store can not be null");
        }
        this.replicationListener = replicationListener;
        this.master = master;
        this.slave = slave;
        this.store = store;
        this.storeUnsafe = storeUnsafe;
        // 主从复制配置
        this.config = config;
        // netty客户端配置
        this.nettyClientConfig = new ClientConfig();
        nettyClientConfig.setSocketBufferSize(1024000);
        // 设置同步数据包大小
        this.nettyClientConfig.setFrameMaxSize(this.config.getBlockSize());
        //this.nettyClientConfig.setEpoll(brokerConfig.getNettyServerConfig().isEpoll());
        // 创建会话
        this.session = new ReplicaImpl(this.slave, null, Replica.State.Offline);
    }

    public void addListener(EventListener<ReplicaEvent> listener) {
        eventBus.addListener(listener);
    }

    public void removeListener(EventListener<ReplicaEvent> listener) {
        eventBus.removeListener(listener);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        // 创建调度计划池
        schedule = Executors.newScheduledThreadPool(1, new NamedThreadFactory
                ("JMQ_SERVER_REPLICATION_SLAVE_SCHEDULE"));
        // 创建客户端
        nettyClient = new NettyClient(nettyClientConfig);
        nettyClient.setFactory(new ReplicationHandlerFactory());

        // 启动客户端
        nettyClient.start();
        // 添加对连接异常的监听
        nettyClient.addListener(new NettyEventListener());
        // 构造master的地址
        address = new InetSocketAddress(master.getIp(), master.getReplicationPort());
        // 创建异步启动任务
        doConnect(0);
        logger.info("replication slave is started");

        if (replicationListener != null) {
            replicationListener.onStart();
        }
    }

    /**
     * 停掉服务
     */
    @Override
    protected void doStop() {
        if (replicationListener != null) {
            replicationListener.onStop();
        }
        super.doStop();
        schedule = null;
        nettyClient = null;

        // 关闭当前数据文件和网络服务
        session.setState(Replica.State.Offline);
        doDisconnect();

        logger.info("replication slave is stopped");
    }

    public Replica getSession() {
        return session;
    }

    /**
     * 创建复制通道，并开启
     *
     * @throws JMQException
     */
    protected void doConnect(int delay) {
        if (!isStarted() || connectFuture != null) {
            return;
        }

        connectFuture = schedule.schedule(new Runnable() {
            @Override
            public void run() {
                session.setState(Replica.State.Offline);

                try {

                    // 建立连接
                    Transport transport = nettyClient.createTransport(address);
                    // 设置会话传输通道
                    session.setTransport(transport);
                    // 同步发送握手信号
                    Command command = transport.sync(new Command(new JMQHeader(Direction.REQUEST, IDENTITY), new Identity().broker(slave)), 0);

                    JMQHeader retHeader = ((JMQHeader) command.getHeader());
                    if (retHeader.getStatus() != JMQCode.SUCCESS.getCode()) {
                        throw new JMQException(retHeader.getError(), retHeader.getStatus());
                    }
                    // 设置会话状态为已连接
                    session.setState(Replica.State.Online);
                    eventBus.add(new ReplicaEvent(State.esAddReplica, 0, session));
                    connectFuture = null;
                    if (replicationListener != null) {
                        replicationListener.onAddReplica(session);
                    }

                    logger.info(String.format("slave success connecting master %s:%d", master.getIp(),
                            master.getReplicationPort()));

                    // 获取本地偏移量
                    long startOffset = store.getMaxOffset();
                    // 创建获取偏移量的任务，需要master计算复制的起始偏移位置
                    sendGetJournal(startOffset, 0);
                    connectionError = 0;
                } catch (Exception e) {
                    // 避免阻塞，不能使用重试
                    connectionError++;
                    if (connectionError > 30) {
                        connectionError = 10;
                    }
                    logger.error("slave start error, will start in " + config.getRestartDelay() * connectionError + "(ms)", e);
                    session.close();
                    // 连接不成功，重连
                    connectFuture = schedule.schedule(this, config.getRestartDelay() * connectionError, TimeUnit.MILLISECONDS);
                }
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * 关闭当前连接，重连
     */
    protected void doReConnect() {
        if (!isStarted()) {
            return;
        }

        replicationPerformanceTrace = null;

        // 增加数据版本
        version.incrementAndGet();
        // 停止
        doDisconnect();
        logger.info(String.format("replication transport to %s:%d will restart in %d(ms)", master.getIp(),
                master.getReplicationPort(), config.getRestartDelay()));
        // 延迟启动2秒，解决Master不能及时感知Slave连接全部断开
        doConnect(2000);
    }

    /**
     * 关闭传输通道、索引更新器，以及存储
     */
    protected void doDisconnect() {
        session.close();
        if (connectFuture != null) {
            connectFuture.cancel(true);
            connectFuture = null;
        }
        eventBus.add(new ReplicaEvent(State.esRemoveReplica, 0, session));
        if (replicationListener != null) {
            replicationListener.onRemoveReplica(session);
        }
    }

    /**
     * 发送获取数据的命令
     *
     * @param offset    偏移量
     * @param maxOffset 最大偏移量
     * @throws JMQException
     */
    protected void sendGetJournal(long offset, long maxOffset) throws JMQException, TransportException {
        if (!isStarted()) {
            return;
        }
        // 创建获取数据的命令
        GetJournal getJournal = new GetJournal().offset(offset).maxOffset(maxOffset).pullTimeout(config.getLongPullTimeout());
        Command command =  new Command(new JMQHeader(Direction.REQUEST, GET_JOURNAL), getJournal);
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("send GET_JOURNAL command to master %s. offset=%d, maxOffset=%d, pullTimeout=%d",
                    this.master.getName(), getJournal.getOffset(), getJournal.getMaxOffset(), getJournal.getPullTimeout()));
        }
        // 异步发送获取数据的命令
        session.getTransport().async(command, getJournal.getPullTimeout(), slaveCallback);
    }

    /**
     * 获取数据的应答
     *
     * @param command 应答命令
     * @throws com.ipd.jmq.common.exception.JMQException
     */
    protected void onCommand(final GetJournalAck command) throws JMQException, TransportException {
        long offset = command.getOffset();
        RByteBuffer refBuf = command.getBuffer();
        ByteBuffer buf = (refBuf == null ? null : refBuf.getBuffer());
        int len = buf == null ? 0 : buf.remaining();

        if (len == 0) {
            if (offset > store.getMaxOffset()) {
                logger.warn("slave behind master too long: slaveMaxOffset={}, masterOffset={}", store.getMaxOffset(), offset);
                if (storeUnsafe != null) {
                    try {
                        storeUnsafe.truncate(offset);
                    } catch (IOException e) {
                        logger.error("cannot truncate journal on {}:", offset, e);
                        doReConnect();
                        return;
                    }
                }
            } else if (offset < store.getMinOffset()) {
                logger.warn("slave maybe not compatible with master: slaveMinOffset={}, masterOffset={}", store.getMinOffset(), offset);
                // TODO: 2016/12/16 clean slave or manual process
                doReConnect();
                return;
            }
            if (config.getLongPullTimeout() > 0) {
                // 长轮询返回空数据，则继续下次轮询请求
                sendGetJournal(offset, offset);
            }
            return;
        }

        if (command.getChecksum() > 0 && buf != null) {
            Checksum checksum = new CRC32();
            ByteBuffer target = buf.slice();
            while (target.hasRemaining()) {
                checksum.update(target.get());
            }

            if (checksum.getValue() != command.getChecksum()) {
                logger.error(String.format("get journal ack checksum error. offset:%d,totalSize:%d,old:%d,new:%d", offset,
                        buf.remaining(), command.getChecksum(), checksum.getValue()));
                doReConnect();
                return;
            }
        }

        try {
            if (!isStarted()) {
                // 已经关闭了
                return;
            }
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("GetJournalAck offset: %d, totalSize:%d", offset, len));
            }
            store.writeMessageBlock(offset, refBuf, true);

            if (replicationPerformanceTrace == null) {
                replicationPerformanceTrace = new ReplicationPerformanceTrace(offset);
            }
            replicationPerformanceTrace.add(len);

            // 下一次取数据的开始位置
            long nextOffset = offset + len;
            updateReplicatedOffset(nextOffset, command.getInsync());

            // 本次数据同步已完成，继续请求剩余数据
            sendGetJournal(nextOffset, nextOffset);
        } catch (Exception e) {
            logger.error("process GetJournalAck error:", e);
        }
    }

    private void updateReplicatedOffset(long offset, boolean insync) {
        if (offset > session.getReplicatedOffset()) {
            session.setReplicatedOffset(offset);
            if (insync) {
                session.setState(Replica.State.Insync);
            } else {
                session.setState(Replica.State.Online);
            }
            eventBus.add(new ReplicaEvent(insync ? State.esInsync : State.esSynchronizing, offset, session));
        }
    }

    /**
     * 处理工厂类
     */
    protected class ReplicationHandlerFactory implements CommandHandlerFactory {
        @Override
        public CommandHandler getHandler(Command command) {
            JMQHeader header = (JMQHeader) command.getHeader();
            return null;
        }
    }

    /**
     * 异步请求回调
     */
    protected class SlaveCallback implements CommandCallback {
        @Override
        public void onSuccess(final Command request, final Command response) {
            if (!isStarted()) {
                return;
            }
            JMQHeader header = (JMQHeader) response.getHeader();
            try {
                switch (header.getType()) {
                    case BOOLEAN_ACK:
                        if (header.getStatus() != JMQCode.SUCCESS.getCode()) {
                            throw new JMQException(header.getError(), header.getStatus());
                        }
                        break;
                    case GET_JOURNAL_ACK:
                        onCommand((GetJournalAck) response.getPayload());
                        break;
                }
            } catch (JMQException e) {
                // TODO 存储异常需要特殊处理
                logger.error(response + ",reconnect in " + config.getRestartDelay() + "(ms)", e);
                //创建异步重启
                doReConnect();
            } catch (Exception e) {
                logger.error(response + " ", e);
                //创建异步重启
                doReConnect();
            }
        }

        @Override
        public void onException(final Command request, final Throwable cause) {
            logger.error("reconnect in " + config.getRestartDelay() + "(ms)", cause);
            //创建异步重启
            doReConnect();
        }
    }

    /**
     * 复制session管理器
     */
    protected class NettyEventListener implements EventListener<TransportEvent> {
        @Override
        public void onEvent(TransportEvent event) {
            if (event.getType() == TransportEvent.EventType.CLOSE || event
                    .getType() == TransportEvent.EventType.EXCEPTION) {
                // 会自动关闭该通道
                Transport transport = event.getTransport();
                if (transport != null && transport.equals(session.getTransport())) {
                    // 去掉对传输通道的引用
                    session.close();

                    if (isStarted()) {
                        logger.info(String.format("replication transport to %s:%d is disconnected, restart in %d(ms)",
                                master.getIp(), master.getReplicationPort(), config.getRestartDelay()));
                        // 创建异步重启
                        doReConnect();
                    }
                }
            }
        }
    }


    /**
     * 跟踪复制数据性能
     */
    protected class ReplicationPerformanceTrace {
        //偏移量
        private long offset;
        private long totalSize;
        //时间戳
        private long startTimestamp;
        private long logTimestamp;

        public ReplicationPerformanceTrace(long offset) {
            this.offset = offset;
            this.startTimestamp = logTimestamp = SystemClock.getInstance().getTime();
        }

        public void add(long size) {
            totalSize += size;
            long deltaTime = SystemClock.getInstance().getTime() - logTimestamp;
            if (deltaTime > 1000 || (logger.isDebugEnabled() && deltaTime > 1000)) {
                logTimestamp = SystemClock.getInstance().getTime();
                double speed = ((double) totalSize / 1000) / (SystemClock.getInstance().getTime() - startTimestamp);
                logger.info("replication on speed: {} MB/s, {}", speed, totalSize);
            }
        }

        public long getOffset() {
            return offset;
        }

        public long getTimestamp() {
            return startTimestamp;
        }
    }

    protected static Logger logger = LoggerFactory.getLogger(ReplicationSlaveService.class);
    // 重启的版本
    protected final AtomicLong version = new AtomicLong(0);
    // 主节点
    protected Broker master;
    // 从节点
    protected Broker slave;
    // 主从复制配置
    protected ReplicationConfig config;
    // 复制系统事件监听器
    protected ReplicationListener replicationListener;
    // 调度计划
    protected ScheduledExecutorService schedule;
    // 复制会话
    protected ReplicaImpl session;
    // 传输通道异步请求回调类
    protected SlaveCallback slaveCallback = new SlaveCallback();
    // netty客户端配置
    protected ClientConfig nettyClientConfig;
    // 传输客户端
    protected NettyClient nettyClient;
    // master地址
    protected SocketAddress address;
    // 存储
    protected Store store;
    // 连接主节点错误次数，用于控制重连时间
    protected long connectionError = 0;
    private final StoreUnsafe storeUnsafe;
    private EventBus<ReplicaEvent> eventBus = new EventBus<ReplicaEvent>("ReplicationSlave.Event");
    private ScheduledFuture connectFuture;
    private ReplicationPerformanceTrace replicationPerformanceTrace;
}