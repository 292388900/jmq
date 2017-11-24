package com.ipd.jmq.client.connection;

import com.ipd.jmq.client.exception.ExceptionHandler;
import com.ipd.jmq.common.cluster.BrokerGroup;
import com.ipd.jmq.common.cluster.Permission;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.network.*;
import com.ipd.jmq.common.network.v3.command.AddConnection;
import com.ipd.jmq.common.network.v3.command.JMQHeader;
import com.ipd.jmq.common.network.v3.command.RemoveConnection;
import com.ipd.jmq.common.network.v3.session.ClientId;
import com.ipd.jmq.common.network.v3.session.ConnectionId;
import com.ipd.jmq.common.model.Acknowledge;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.toolkit.network.Ipv4;
import com.ipd.jmq.toolkit.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分组故障转移传输通道
 *
 * @author lindeqiang
 * @since 14-4-29 上午10:37
 */

public abstract class GroupClient extends FailoverClient implements GroupTransport {
    private static Logger logger = LoggerFactory.getLogger(GroupClient.class);
    // 传输通道配置
    protected TransportConfig config;
    // broker分组
    protected BrokerGroup group;
    // 主题
    protected String topic;
    // 客户端ID
    protected ClientId clientId;
    // 连接标识
    protected ConnectionId connectionId;
    // 权限
    protected Permission permission = Permission.NONE;
    // 数据中心
    protected byte dataCenter;

    public GroupClient(Client client, FailoverPolicy failoverPolicy, RetryPolicy retryPolicy) {
        super(client, failoverPolicy, retryPolicy);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setConfig(TransportConfig config) {
        this.config = config;
    }

    @Override
    public ConnectionId getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(ConnectionId connectionId) {
        this.connectionId = connectionId;
    }

    public BrokerGroup getGroup() {
        return group;
    }

    public void setGroup(BrokerGroup group) {
        this.group = group;
    }

    public void setClientId(ClientId clientId) {
        this.clientId = clientId;
    }

    public void setDataCenter(byte dataCenter) {
        this.dataCenter = dataCenter;
    }

    @Override
    protected void validate() throws Exception {
        if (group == null) {
            throw new IllegalStateException("group can not be null");
        }
        if (topic == null) {
            throw new IllegalStateException("topic can not be null");
        }
        if (clientId == null) {
            throw new IllegalStateException("clientId can not be null");
        }
        if (failoverPolicy == null) {
            failoverPolicy = new GroupFailoverPolicy(group, permission);
        }
        super.validate();
    }

    @Override
    protected boolean login(Transport transport) throws TransportException {
        ConnectionId newId;
        // 创建连接ID
        if (connectionId == null) {
            newId = new ConnectionId(clientId);
        } else {
            // 每次重连增加序号
            newId = new ConnectionId(connectionId.getClientId(), connectionId.incrSequence());
        }
        AddConnection addConnection =
                new AddConnection().app(config.getApp()).connectionId(newId).user(config.getUser())
                        .password(config.getPassword());
        //String version = this.getClass().getPackage().getImplementationVersion();
        String version = GetVersionUtil.getClientVersion();

        addConnection.setClientVersion(version);

        //发送创建连接命令
        Command request = new Command(
                JMQHeader.Builder.request(addConnection.type(), Acknowledge.ACK_RECEIVE), addConnection);
        Command addAck = transport.sync(request, config.getSendTimeout());
        JMQHeader header = (JMQHeader) addAck.getHeader();
        int status = header.getStatus();
        if (status == JMQCode.CN_AUTHENTICATION_ERROR.getCode()) {
            String remoteIp = null;
            try {
                remoteIp = Ipv4.toAddress(transport.remoteAddress());
            } catch (Exception ignored) {
            }
            // 没有权限记录日志继续重连
            // 认证失败
            String exceptionMessage = JMQCode.CN_AUTHENTICATION_ERROR.getMessage();
            String solution = ExceptionHandler.getExceptionSolution(exceptionMessage);
            logger.error(exceptionMessage + ", {}, check your app&password.", remoteIp + solution);
        }
        if (status == JMQCode.SUCCESS.getCode()) {
            // 成功连上，设置新的连接ID
            connectionId = newId;
            return true;
        }
        return false;
    }

    @Override
    public int getWeight() {
        if (state.get() == FailoverState.CONNECTED) {
            if (group != null) {
                return group.getWeight();
            }
            return 100;
        }
        return 0;
    }

    /**
     * 设置权重
     *
     * @param weight 权重
     */
    public void setWeight(final short weight) {
        if (group != null && group.getWeight() != weight) {
            group.setWeight(weight);
        }
    }

    @Override
    protected void doStop() {
        // 防止Channel为空
        Transport transport = this.transport;
        ConnectionId connectionId = this.connectionId;
        if (transport != null && connectionId != null) {
            try {
                //发送销毁连接命令
                RemoveConnection removeConnection = new RemoveConnection().connectionId(connectionId);
                Command request = new Command(
                        JMQHeader.Builder.request(removeConnection.type(), Acknowledge.ACK_NO), removeConnection);
                transport.oneway(request, config.getSendTimeout());
            } catch (Throwable ignored) {
            }
        }
        super.doStop();
    }
}