package com.ipd.jmq.server.broker.handler;

import com.ipd.jmq.common.cluster.Permission;
import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.network.v3.command.*;
import com.ipd.jmq.common.network.v3.session.*;
import com.ipd.jmq.toolkit.security.auth.Authentication;
import com.ipd.jmq.toolkit.security.auth.UserDetails;
import com.ipd.jmq.common.stat.ClientTPStat;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.SessionManager;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.broker.monitor.BrokerMonitor;
import com.ipd.jmq.server.broker.profile.ClientStatManager;
import com.ipd.jmq.server.broker.utils.BrokerUtils;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.network.Ipv4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.ExecutorService;


/**
 * 会话管理
 */
public class SessionHandler extends AbstractHandler implements JMQHandler {
    private static final Logger logger = LoggerFactory.getLogger(SessionHandler.class);
    private BrokerConfig config;
    private BrokerMonitor brokerMonitor;
    private ClusterManager clusterManager;
    private Authentication authentication;
    private ClientStatManager clientStatManager;
    private static String NEED_MIN_VERSION = "2.0.0";
    public SessionHandler(){}
    public SessionHandler(ExecutorService executorService, SessionManager sessionManager, ClusterManager clusterManager,
                          final BrokerConfig config, ClientStatManager clientStatManager, BrokerMonitor brokerMonitor) {

        Preconditions.checkArgument(config != null, "config can not be null");
        Preconditions.checkArgument(brokerMonitor != null, "brokerMonitor can not be null");
        Preconditions.checkArgument(clusterManager != null, "clusterManager can not be null");
        Preconditions.checkArgument(sessionManager != null, "sessionManager can not be null");
        Preconditions.checkArgument(executorService != null, "executorService can not be null");

        this.config = config;
        this.broker = clusterManager.getBroker();
        this.brokerMonitor = brokerMonitor;
        this.sessionManager = sessionManager;
        this.clusterManager = clusterManager;
        this.authentication = config.getAuthentication();
        this.executorService = executorService;
        this.clientStatManager = clientStatManager;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }


    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }
    public void setConfig(BrokerConfig config) {
        this.config = config;
        this.authentication = config.getAuthentication();
    }

    public void setBrokerMonitor(BrokerMonitor brokerMonitor) {
        this.brokerMonitor = brokerMonitor;
    }

    public void setClusterManager(ClusterManager clusterManager) {
        this.broker = clusterManager.getBroker();
        this.clusterManager = clusterManager;
    }

    public void setClientStatManager(ClientStatManager clientStatManager) {
        this.clientStatManager = clientStatManager;
    }

    /**
     * 心跳监控
     *
     * @param request   请求命令
     * @param payload   命令实体
     * @return 应答命令
     * @throws JMQException
     */
    protected Command execute( final Command request, final Heartbeat payload) throws
            JMQException {
        // 心跳不需要应答
        return CommandUtils.createBooleanAck(request.getHeader().getRequestId(), JMQCode.SUCCESS);
    }

    /**
     * 生产者获取集群状态
     *
     * @param request   请求命令
     * @param payload   命令实体
     * @throws JMQException
     */
    protected Command execute(final Command request, final GetProducerHealth payload) throws JMQException {
        Command getHealthAck;
        Permission permission =
                clusterManager.getPermission(payload.getTopic(), payload.getApp(), payload.getDataCenter());
        Header header = request.getHeader();
        if (!permission.contain(Permission.WRITE)) {
            getHealthAck = CommandUtils.createBooleanAck(request.getHeader().getRequestId(), JMQCode.CN_NO_PERMISSION.getCode(),
                    JMQCode.CN_NO_PERMISSION.getMessage());
        } else if (sessionManager.getProducerById(payload.getProducerId()) == null) {
            getHealthAck = CommandUtils.createBooleanAck(header.getRequestId(), JMQCode.FW_PRODUCER_NOT_EXISTS.getCode(),
                    JMQCode.FW_PRODUCER_NOT_EXISTS.getMessage());
        } else {
            TopicConfig topicConfig = clusterManager.getTopicConfig(payload.getTopic());
            if ((topicConfig == null) || (topicConfig.getImportance() != TopicConfig.TopicVeryImportant) ||
                    brokerMonitor.hasReplicas()) {
                getHealthAck = CommandUtils.createBooleanAck(header.getRequestId(), JMQCode.SUCCESS.getCode(), JMQCode.SUCCESS.getMessage());
            } else {
                getHealthAck = CommandUtils.createBooleanAck(header.getRequestId(), JMQCode.CY_REPLICATE_ERROR.getCode(),
                        JMQCode.CY_REPLICATE_ERROR.getMessage());
            }
        }

        return getHealthAck;
    }

    /**
     * 消费者者获取集群状态。
     *
     * @param request   请求命令
     * @param payload   命令实体
     * @throws JMQException
     */
    protected Command execute(final Command request, final GetConsumerHealth payload) throws JMQException {
        Command getHealthAck;
        Permission permission =
                clusterManager.getPermission(payload.getTopic(), payload.getApp(), payload.getDataCenter());
        Header header = request.getHeader();
        if (!permission.contain(Permission.READ)) {
            getHealthAck = CommandUtils.createBooleanAck(header.getRequestId(), JMQCode.CN_NO_PERMISSION.getCode(),
                    JMQCode.CN_NO_PERMISSION.getMessage());
        } else if (sessionManager.getConsumerById(payload.getConsumerId()) == null) {
            getHealthAck = CommandUtils.createBooleanAck(header.getRequestId(), JMQCode.FW_CONSUMER_NOT_EXISTS.getCode(),
                    JMQCode.FW_CONSUMER_NOT_EXISTS.getMessage());
        } else {
            getHealthAck = CommandUtils.createBooleanAck(header.getRequestId(), JMQCode.SUCCESS.getCode(),
                    JMQCode.SUCCESS.getMessage());
        }

        return getHealthAck;
    }

    /**
     * 客户端性能统计
     *
     * @param transport 连接通道
     * @param request   请求命令
     * @param payload   命令实体
     * @throws JMQException
     */
    protected Command execute(final Transport transport, final Command request, final ClientProfile payload) throws JMQException {
        List<ClientTPStat> clientStats = payload.getClientStats();
        if (clientStats != null) {
            String ip;
            SocketAddress address = transport.remoteAddress();
            if (address instanceof InetSocketAddress) {
                ip = ((InetSocketAddress) address).getAddress().getHostAddress();
            } else {
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 ip = address.toString();
            }
            for (ClientTPStat clientStat : clientStats) {
                clientStat.setIp(ip);
                if (logger.isDebugEnabled()) {
                    logger.debug(clientStat.toString());
                }
            }
        }
        if (clientStatManager != null) {
            clientStatManager.update(clientStats);
        }

        byte opener = (byte) ( (config.getClientTpFlag() & 0x1) | ((config.getClientMetricsFlag() << 1) & 0x2) );

        ClientProfileAck ack = new ClientProfileAck()
                .interval(config.getClientProfileInterval())
                .machineMetricsInterval(config.getClientMachineMetricsInterval())
                .opener(opener);

        return new Command(JMQHeader.Builder.create().type(ack.type()).
                direction(Direction.RESPONSE).
                requestId(request.getHeader().getRequestId()).
                status(JMQCode.SUCCESS.getCode()).build(), ack);
    }

    /**
     * 添加链接。
     *
     * @param transport 连接通道
     * @param request   请求命令
     * @param payload   命令实体
     * @throws JMQException
     */
    protected Command execute(final Transport transport, final Command request, final AddConnection payload) throws JMQException {
        // broker被禁用了
        if (broker.isDisabled()) {
            throw new JMQException("broker is disabled", JMQCode.CN_NO_PERMISSION.getCode());
        }
        UserDetails user = authentication.getUser(payload.getUser());

        // user认证
        if (user == null || !user.isEnabled() || user.isExpired() || user.isLocked()) {
            throw new JMQException(JMQCode.CN_AUTHENTICATION_ERROR.getMessage() + " user:" + payload.getUser(),
                    JMQCode.CN_AUTHENTICATION_ERROR.getCode());
        }

        String password =payload.getPassword();// authentication.getPasswordEncode().encode();
        if (!user.getPassword().equalsIgnoreCase(password)) {
            Command response = CommandUtils.createBooleanAck(request.getHeader().getRequestId(), JMQCode.CN_AUTHENTICATION_ERROR);
            String error = JMQCode.CN_AUTHENTICATION_ERROR.getMessage() + " user:" + payload.getUser();
            ((JMQHeader) response.getHeader()).setError(error);
            logger.warn(error);
            return response;
        }

        Connection connection = new Connection();
        connection.setTransport(transport);
        connection.setApp(payload.getApp());
        connection.setId(payload.getConnectionId().getConnectionId());
        connection.setLanguage(payload.getLanguage());
        connection.setVersion(payload.getClientVersion());

        connection.setAddress(Ipv4.toByte((InetSocketAddress) transport.remoteAddress()));
        connection.setServerAddress(Ipv4.toByte(broker.getName()));
        if (!sessionManager.addConnection(connection)) {
            logger.warn(String.format("connection maybe already exists. app=%s address=%s id=%s", connection.getApp(),
                    Ipv4.toAddress(transport.remoteAddress()), connection.getId()));
        }
        // 绑定连接和用户
        transport.attr(SessionManager.CONNECTION_KEY, connection);
        transport.attr(SessionManager.USER_KEY, user);
        return CommandUtils.createBooleanAck(request.getHeader().getRequestId(), JMQCode.SUCCESS);
    }


    /**
     * 删除链接。
     *
     * @param request   请求命令
     * @param payload   命令实体
     * @throws JMQException
     */
    protected Command execute(final Command request, final RemoveConnection payload) throws JMQException {
        sessionManager.removeConnection(payload.getConnectionId().getConnectionId());
        return CommandUtils.createBooleanAck(request.getHeader().getRequestId(), JMQCode.SUCCESS);
    }

    /**
     * 添加生产者。
     *
     * @param request   请求命令
     * @param payload   命令实体
     * @throws JMQException
     */
    protected Command execute(final Command request, final AddProducer payload) throws JMQException {
        String topic = payload.getTopic();
        ProducerId producerId = payload.getProducerId();
        ConnectionId connectionId = producerId.getConnectionId();
        Connection connection = sessionManager.getConnectionById(connectionId.getConnectionId());
        // 链接不存在
        if (connection == null) {
            throw new JMQException(
                    String.format("connection %s is not exists. topic:%s", connectionId.getConnectionId(), topic),
                    JMQCode.FW_CONNECTION_NOT_EXISTS.getCode());
        }
        // Broker被禁用了或不能发送消息
        clusterManager.checkWritable(topic, connection.getApp());
        TopicConfig topicConfig = clusterManager.getTopicConfig(topic);
        if (!checkClientVersion(topicConfig, connection.getApp(), connection.getVersion())) {
            return CommandUtils.createBooleanAck(request.getHeader().getRequestId(), JMQCode.CT_LOW_VERSION.getCode(),
                    String.format("current client version %s less than minVersion %s for using broadcast or sequential!", connection.getVersion(), NEED_MIN_VERSION));
        }

        if (topicConfig != null && topicConfig.checkSequential() && topicConfig.singleProducer(connection.getApp())) {
            sessionManager.checkAndBindSequentialProducer(connection.getApp(), topic, producerId.getProducerId());
        }

        Producer producer = new Producer(producerId.getProducerId(), connectionId.getConnectionId(), topic);
        producer.setApp(connection.getApp());
        if (!sessionManager.addProducer(producer)) {
            return CommandUtils.createBooleanAck(request.getHeader().getRequestId(), JMQCode.CN_UNKNOWN_ERROR);
        }
        return CommandUtils.createBooleanAck(request.getHeader().getRequestId(), JMQCode.SUCCESS);
    }

    private boolean checkClientVersion(TopicConfig topicConfig, String app, String clientVersion) {
        if (topicConfig != null && (topicConfig.isLocalManageOffsetConsumer(app) || topicConfig.checkSequential())) {
            int isLowClientVersion;
            isLowClientVersion = BrokerUtils.compareVersion(clientVersion, NEED_MIN_VERSION);
            if (isLowClientVersion < 0) {
                if (logger.isDebugEnabled()) {
                    logger.error(String.format("current client version %s less than minVersion %s for using broadcast or sequential!", clientVersion, NEED_MIN_VERSION));
                }
                return false;
            }
        }
        return true;
    }


    /**
     * 删除生产者。
     *
     * @param request   请求命令
     * @param payload   命令实体
     * @throws JMQException
     */
    protected Command execute(final Command request, final RemoveProducer payload) throws JMQException {
        ProducerId producerId = payload.getProducerId();
        Producer producer = sessionManager.getProducerById(producerId.getProducerId());
        if (producer != null) {
            sessionManager.removeProducer(producer.getId());
        }
        return CommandUtils.createBooleanAck(request.getHeader().getRequestId(), JMQCode.SUCCESS);
    }

    /**
     * 添加消费者。
     *
     * @param request   请求命令
     * @param payload   命令实体
     * @throws JMQException
     */
    protected Command execute(final Command request, final AddConsumer payload) throws JMQException {
        String topic = payload.getTopic();
        ConsumerId consumerId = payload.getConsumerId();
        ConnectionId connectionId = consumerId.getConnectionId();
        Connection connection = sessionManager.getConnectionById(connectionId.getConnectionId());
        // 链接不存在
        if (connection == null) {
            throw new JMQException(
                    String.format("connection %d is not exists. topic:%s", connectionId.getConnectionId(), topic),
                    JMQCode.FW_CONNECTION_NOT_EXISTS.getCode());
        }
        // 验证权限
        clusterManager.checkReadable(topic, connection.getApp());
        // 验证客户端版本是否符合要求,低于2.0.0版本客户端禁止使用广播和严格顺序消费功能
        TopicConfig topicConfig = clusterManager.getTopicConfig(topic);
        if (!checkClientVersion(topicConfig, connection.getApp(), connection.getVersion())) {
            return CommandUtils.createBooleanAck(request.getHeader().getRequestId(), JMQCode.CT_LOW_VERSION.getCode(),
                    String.format("current client version %s less than minVersion %s for using broadcast or sequential!", connection.getVersion(), NEED_MIN_VERSION));
        }
        // 创建消费者
        Consumer consumer =
                new Consumer(consumerId.getConsumerId(), connectionId.getConnectionId(), topic, payload.getSelector());
        consumer.setApp(connection.getApp());
        if (!sessionManager.addConsumer(consumer)) {
            return CommandUtils.createBooleanAck(request.getHeader().getRequestId(), JMQCode.CN_UNKNOWN_ERROR);
        }
        return CommandUtils.createBooleanAck(request.getHeader().getRequestId(), JMQCode.SUCCESS);
    }

    /**
     * 删除消费者
     *
     * @param request   请求命令
     * @param payload   命令实体
     * @throws JMQException
     */
    protected Command execute(final Command request, final RemoveConsumer payload) throws JMQException {
        ConsumerId consumerId = payload.getConsumerId();
        Consumer consumer = sessionManager.getConsumerById(consumerId.getConsumerId());
        if (consumer != null) {
            sessionManager.removeConsumer(consumer.getId());
        }
        return CommandUtils.createBooleanAck(request.getHeader().getRequestId(), JMQCode.SUCCESS);
    }

    @Override
    public int[] type() {
        return new int[]{
                CmdTypes.GET_PRODUCER_HEALTH,
                CmdTypes.GET_CONSUMER_HEALTH,
                CmdTypes.ADD_CONNECTION,
                CmdTypes.REMOVE_CONNECTION,
                CmdTypes.ADD_PRODUCER,
                CmdTypes.REMOVE_PRODUCER,
                CmdTypes.ADD_CONSUMER,
                CmdTypes.REMOVE_CONSUMER,
                CmdTypes.CLIENT_PROFILE,
                CmdTypes.HEARTBEAT,
                CmdTypes.VOTE};
    }

    @Override
    public Command process(Transport transport, Command command) throws TransportException {
        try {
            JMQHeader header = (JMQHeader) command.getHeader();
            Object payload = command.getPayload();
            switch (header.getType()) {
                case CmdTypes.GET_PRODUCER_HEALTH:
                    return execute(command, (GetProducerHealth) payload);
                case CmdTypes.GET_CONSUMER_HEALTH:
                    return execute(command, (GetConsumerHealth) payload);
                case CmdTypes.CLIENT_PROFILE:
                    return execute(transport, command, (ClientProfile) payload);
                case CmdTypes.HEARTBEAT:
                    return execute(command, (Heartbeat) payload);
                case CmdTypes.ADD_CONNECTION:
                    return execute(transport, command, (AddConnection) payload);
                case CmdTypes.REMOVE_CONNECTION:
                    return execute(command, (RemoveConnection) payload);
                case CmdTypes.ADD_PRODUCER:
                    return execute( command, (AddProducer) payload);
                case CmdTypes.REMOVE_PRODUCER:
                    return execute(command, (RemoveProducer) payload);
                case CmdTypes.ADD_CONSUMER:
                    return execute(command, (AddConsumer) payload);
                case CmdTypes.REMOVE_CONSUMER:
                    return execute(command, (RemoveConsumer) payload);
                default:
                    throw new JMQException(JMQCode.CN_COMMAND_UNSUPPORTED);
            }

        } catch (JMQException e) {
            throw new TransportException(e.getMessage(), e.getCause(),e.getCode());
        }
    }
}