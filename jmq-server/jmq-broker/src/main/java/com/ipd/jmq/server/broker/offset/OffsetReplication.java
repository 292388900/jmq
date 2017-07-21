package com.ipd.jmq.server.broker.offset;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.cluster.BrokerGroup;
import com.ipd.jmq.common.cluster.OffsetItem;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.network.*;
import com.ipd.jmq.common.network.v3.codec.JMQCodec;
import com.ipd.jmq.common.network.v3.command.*;
import com.ipd.jmq.common.network.v3.netty.NettyClient;
import com.ipd.jmq.common.network.v3.netty.NettyDecoder;
import com.ipd.jmq.common.network.v3.netty.NettyEncoder;
import com.ipd.jmq.server.broker.cluster.ClusterEvent;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.broker.handler.AbstractHandler;
import com.ipd.jmq.server.broker.handler.DefaultHandlerFactory;
import com.ipd.jmq.server.broker.handler.JMQHandler;
import com.ipd.jmq.common.model.Acknowledge;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.common.network.v3.command.Direction;
import com.ipd.jmq.common.network.v3.netty.AbstractServer;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.concurrent.RingBuffer;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.network.Ipv4;
import com.ipd.jmq.toolkit.retry.RetryPolicy;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.service.ServiceThread;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 消费位置复制.
 *
 * @author lindeqiang
 * @since 2016/11/28 11:51
 */
public class OffsetReplication extends Service {
    protected static final String IDENTITY = "offset_identity";
    protected static final Logger logger = LoggerFactory.getLogger(OffsetReplication.class);
    //远端会话缓存(用于被动接收消费位置信息)
    protected ConcurrentMap<Broker, OffsetSession> remoteSessions = new ConcurrentHashMap<Broker, OffsetSession>();
    //本地会话缓存(用户主动获取消费位置信息)
    protected ConcurrentMap<Broker, OffsetSession> localSessions = new ConcurrentHashMap<Broker, OffsetSession>();

    protected RingBuffer offsetSyncBuffer;

    private OffsetServer offsetServer;

    protected Client nettyClient;

    private ClusterManager clusterManager;

    private OffsetManager offsetManager;

    private Thread thread;

    public OffsetReplication(ClusterManager clusterManager, OffsetManager offsetManager) {
        this.clusterManager = clusterManager;
        this.offsetManager = offsetManager;
    }

    /**
     * 启动
     *
     * @throws Exception
     */
    protected void doStart() throws Exception {
        super.doStart();
        // 消费位置处理器工厂
        DefaultHandlerFactory factory = new DefaultHandlerFactory();
        factory.register(new GetConsumeOffsetHandler());
        factory.register(new IdentityHandler());
        factory.register(new ResetAckOffsetHandler());

        Broker broker = clusterManager.getBroker();
        clusterManager.addListener(new BrokerListener());

        ServerConfig serverConfig = new ServerConfig();
        serverConfig.setIp(broker.getIp());
        serverConfig.setPort(broker.getOffsetPort());
        serverConfig.setSelectorThreads(2);
        serverConfig.setWorkerThreads(5);

        // 消费位置服务
        offsetServer = new OffsetServer(serverConfig, factory);
        offsetServer.start();
        // 添加通道监听器
        offsetServer.addListener(new TransportListener());


        offsetSyncBuffer = new RingBuffer(100000, new OffsetSyncHandler(), "JMQ_OFFSET_SYNC_BUFFER");
        offsetSyncBuffer.start();


        // 创建netty客户端
        if (nettyClient == null) {
            nettyClient = new NettyClient(new ClientConfig(), null, null, new OffsetHandlerFactory());
        }
        nettyClient.start();


        thread = new Thread(new ServiceThread(this) {
            @Override
            protected void execute() throws Exception {
                check();
            }

            @Override
            public long getInterval() {
                return 1000 * 30;
            }

            @Override
            public boolean onException(Throwable e) {
                logger.error(e.getMessage(), e);
                return true;
            }

            private void check() {
                handleBrokerChanged(clusterManager.getBrokerGroup().getBrokers());
            }

        }, "JMQ_OFFSET_REP_SESSION_GUARD");
        thread.start();


    }

    /**
     * 复制消费位置
     *
     * @param item
     * @throws JMQException
     */
    public void replicate(OffsetItem item) {
        //没有空间立刻返回
        boolean added = offsetSyncBuffer.add(item, 0);
        if (!added) {
            logger.warn("Try to replicate offset failure. JMQ_OFFSET_SYNC_BUFFER buffer already full.");
        }
    }


    @Override
    protected void doStop() {

        Close.close(offsetServer);
        Close.close(offsetSyncBuffer);

        super.doStop();
        logger.info("offset manager is stopped.");
    }


    // broker变更监听器，感知broker变更动态创建连接或销毁连接
    protected class BrokerListener implements EventListener<ClusterEvent> {
        @Override
        public void onEvent(ClusterEvent event) {
            if (event.getType().equals(ClusterEvent.EventType.ALL_BROKER_UPDATE)) {
                BrokerGroup group = clusterManager.getBrokerGroup();
                List<Broker> brokers = group.getBrokers();
                handleBrokerChanged(brokers);
            }
        }
    }

    // broker变更处理
    private synchronized void handleBrokerChanged(List<Broker> brokers) {
        // 添加新的session
        Broker localBroker = clusterManager.getBroker();
        for (Broker broker : brokers) {
            if (!localSessions.containsKey(broker) && (localBroker != null && !localBroker.equals(broker))) {
                doCreateSession(broker);
            }
        }

        // 移除多余的session
        for (Broker broker : localSessions.keySet()) {
            try {
                if (!brokers.contains(broker)) {
                    OffsetSession session = localSessions.remove(broker);
                    if (session != null) {
                        session.close();
                    }
                }
            } catch (Exception e) {
                logger.warn("remove offset session error!", e);
            }
        }
    }

    // 创建会话
    private void doCreateSession(Broker broker) {
        FailoverClient transport = new FailoverClient(nettyClient, new FixedFailoverPolicy(broker.getOffsetAddress()), new
                RetryPolicy());
        boolean createSuccess = false;
        try {
            OffsetSession session = new OffsetSession(broker, transport);
            transport.attr(IDENTITY, session);
            if (localSessions.putIfAbsent(broker, session) == null) {
                transport.addListener(session);
                transport.start();
            }
            createSuccess = true;
        } catch (Exception e) {
            logger.error("Create offset session error.", e);
        }

        if (!createSuccess) {
            OffsetSession session = localSessions.remove(broker);
            if (session != null) {
                session.close();
            }
        }
    }

    /**
     * 被动连接监听器
     */
    protected class TransportListener implements EventListener<TransportEvent> {
        @Override
        public void onEvent(TransportEvent event) {
            TransportEvent.EventType type = event.getType();
            if (type.equals(TransportEvent.EventType.CLOSE) || type.equals(TransportEvent.EventType.EXCEPTION)) {
                Transport transport = event.getTransport();
                OffsetSession session = (OffsetSession) transport.attr(IDENTITY);
                remoteSessions.remove(session.getBroker());
            }
        }
    }

    protected class OffsetSyncHandler implements RingBuffer.EventHandler {
        @Override
        public void onEvent(Object[] elements) throws Exception {
            if (!isStarted()) {
                return;
            }
            if (!remoteSessions.isEmpty()) {
                ResetAckOffset payload = new ResetAckOffset();
                payload.setItems(Arrays.copyOf(elements, elements.length, OffsetItem[].class));
                Command command = new Command(JMQHeader.Builder.create().direction(Direction.REQUEST).type(payload.type()).acknowledge(Acknowledge.ACK_NO).build(), payload);
                for (OffsetSession session : remoteSessions.values()) {
                    Transport transport = session.getTransport();
                    if (transport != null) {
                        try {
                            transport.oneway(command);
                        } catch (Exception e) {
                            logger.warn("Sync offset to {} failure!", Ipv4.toAddress(transport.remoteAddress()), e);
                        }
                    }
                }
            } else {
                if (logger.isDebugEnabled()){
                    logger.debug("No offset session exists!");
                }
            }
        }

        @Override
        public void onException(Throwable e) {
            logger.error("Process offset sync error.", e);
        }

        @Override
        public int getBatchSize() {
            return 0;
        }

        @Override
        public long getInterval() {
            return 0;
        }
    }


    /**
     * 消费位置接收服务
     */
    protected class OffsetServer extends AbstractServer {
        private JMQCodec codec = new JMQCodec();

        public OffsetServer(ServerConfig config, CommandHandlerFactory factory) {
            this(config, null, null, null, factory);
        }

        public OffsetServer(ServerConfig config, EventLoopGroup bossLoopGroup, EventLoopGroup ioLoopGroup, EventExecutorGroup workerGroup, CommandHandlerFactory factory) {
            super(config, bossLoopGroup, ioLoopGroup, workerGroup, factory);
        }

        @Override
        protected ChannelHandler[] createChannelHandlers() {
            return new ChannelHandler[]{new NettyDecoder(codec), new NettyEncoder(codec), connectionHandler, commandInvocation};
        }
    }

    /**
     * 获取偏移量处理器
     */
    protected class GetConsumeOffsetHandler extends AbstractHandler implements JMQHandler {
        @Override
        public Command process(Transport transport, Command command) throws TransportException {
            if (!isStarted()) {
                if (!isStarted()) {
                    //状态设置为服务不可用
                    return CommandUtils.createBooleanAck(command.getHeader().getRequestId(), JMQCode.CN_SERVICE_NOT_AVAILABLE);
                }
            }

            if (logger.isDebugEnabled()) {
                OffsetSession session = remoteSessions.get(transport);
                Broker broker = null;
                if (session != null) {
                    broker = session.getBroker();
                }
                if (broker != null) {
                    logger.debug("sync ack offset from " + broker.getName());
                } else {
                    logger.debug("sync ack offset from " + Ipv4.toAddress(transport.remoteAddress()));
                }
            }

            GetConsumeOffset payload = (GetConsumeOffset) command.getPayload();
            String offsets = payload.getOffset();

            //如果正在从消费，则合并消费位置并取消从消费
            updateOffset(offsets);

            GetConsumeOffsetAck ack = new GetConsumeOffsetAck();
            ack.setOffset(offsetManager.getConsumeOffset());

            return new Command(JMQHeader.Builder.create().direction(Direction.REQUEST).acknowledge(Acknowledge.ACK_NO)
                    .type(ack.type()).build(), ack);
        }


        //更新消费位置
        private void updateOffset(String offsetContent) {
            if (offsetContent == null || offsetContent.isEmpty()) {
                return;
            }

            Map<String, TopicOffset> offsets =
                    JSON.parseObject(offsetContent, new TypeReference<ConcurrentMap<String, TopicOffset>>() {
                    });
            offsetManager.updateOffset(offsets);
        }

        @Override
        public int[] type() {
            return new int[]{CmdTypes.GET_CONSUMER_OFFSET};
        }
    }

    /**
     * 连接认证处理器
     */
    protected class IdentityHandler extends AbstractHandler implements JMQHandler {
        @Override
        public Command process(Transport transport, Command command) throws TransportException {
            if (!isStarted()) {
                //状态设置为服务不可用
                return CommandUtils.createBooleanAck(command.getHeader().getRequestId(), JMQCode.CN_SERVICE_NOT_AVAILABLE);
            }

            // 身份信息
            Identity identity = (Identity) command.getPayload();
            Broker remoteBroker = identity.getBroker();
            List<Broker> brokers = clusterManager.getBrokerGroup().getBrokers();
            Command response;

            if (!brokers.contains(remoteBroker)) {
                response = CommandUtils.createBooleanAck(command.getHeader().getRequestId(), JMQCode.CN_NO_PERMISSION);
            } else {
                // 获取绑定的复制会话
                OffsetSession session = (OffsetSession) transport.attr(IDENTITY);
                if (session == null) {
                    session = new OffsetSession(remoteBroker, transport, OffsetSession.Status.CONNECTED);
                }
                transport.attr(IDENTITY, session);
                remoteSessions.put(identity.getBroker(), session);

                logger.info(String.format("%s was connected", remoteBroker.getName()));
                // 状态设置为成功
                response = CommandUtils.createBooleanAck(command.getHeader().getRequestId(), JMQCode.SUCCESS);
            }

            return response;
        }

        @Override
        public int[] type() {
            return new int[]{CmdTypes.IDENTITY};
        }
    }

    /**
     * 重置消费位置处理器工厂
     */
    private class OffsetHandlerFactory implements CommandHandlerFactory {
        CommandHandler resetOffsetHandler = new ResetAckOffsetHandler();

        @Override
        public CommandHandler getHandler(Command command) {
            return resetOffsetHandler;
        }
    }

    /**
     * 重置消费位置处理器
     */
    private class ResetAckOffsetHandler extends AbstractHandler implements JMQHandler {
        @Override
        public Command process(Transport transport, Command command) throws TransportException {
            ResetAckOffset payload = (ResetAckOffset) command.getPayload();
            OffsetItem[] setters = payload.getItems();

            if (setters != null) {
                for (OffsetItem setter : setters) {
                    offsetManager.resetAckOffset(setter.getTopic(), setter.getQueueId(), setter.getApp(), setter
                            .getQueueOffset(), false);
                }
            }
            return null;
        }

        @Override
        public int[] type() {
            return new int[]{CmdTypes.RESET_ACK_OFFSET};
        }
    }
}