package com.ipd.jmq.server.broker.handler;

import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.network.v3.command.*;
import com.ipd.jmq.common.network.v3.session.Consumer;
import com.ipd.jmq.common.network.v3.session.ConsumerId;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.LongPullManager;
import com.ipd.jmq.server.broker.SessionManager;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
import com.ipd.jmq.server.broker.dispatch.PullResult;
import com.ipd.jmq.server.broker.monitor.BrokerMonitor;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.common.network.command.CommandCallback;
import com.ipd.jmq.common.network.command.Direction;
import com.ipd.jmq.common.network.command.Header;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.time.MilliPeriod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;


/**
 * 获取消息处理器
 */
public class GetMessageHandler extends AbstractHandler implements JMQHandler  {
    protected static Logger logger = LoggerFactory.getLogger(GetMessageHandler.class);
    protected DispatchService dispatchService;
    protected LongPullManager longPullManager;
    protected ClusterManager clusterManager;
    protected BrokerMonitor brokerMonitor;

    public GetMessageHandler(ExecutorService executorService, SessionManager sessionManager,
                             ClusterManager clusterManager, DispatchService dispatchService, LongPullManager longPullManager,
                             BrokerMonitor brokerMonitor, BrokerConfig brokerConfig) {

        Preconditions.checkArgument(brokerMonitor != null , "brokerMonitor can not be null");
        Preconditions.checkArgument(clusterManager != null , "clusterManager can not be null");
        Preconditions.checkArgument(sessionManager != null , "sessionManager can not be null");
        Preconditions.checkArgument(dispatchService != null , "dispatchService can not be null");
        Preconditions.checkArgument(executorService != null , "executorService can not be null");
        Preconditions.checkArgument(longPullManager != null , "longPullManager can not be null");


        this.broker = clusterManager.getBroker();
        this.brokerMonitor = brokerMonitor;
        this.sessionManager = sessionManager;
        this.clusterManager = clusterManager;
        this.dispatchService = dispatchService;
        this.longPullManager = longPullManager;
        this.executorService = executorService;
    }


    protected JMQHeader createAckHeader(final Command request) {
        return new JMQHeader(Direction.RESPONSE, CmdTypes.GET_MESSAGE_ACK, request.getHeader().getRequestId(),
                JMQCode.SUCCESS.getCode(), JMQCode.SUCCESS.getMessage());

    }

    @Override
    public Command process(Transport transport, Command command) throws TransportException {
        try {

            Header header = command.getHeader();

            GetMessage message = (GetMessage) command.getPayload();
            // 消费者不存在
            ConsumerId consumerId = null;
            Consumer consumer = null;
            Consumer.ConsumeType type = (Consumer.ConsumeType) command.getObject();
            if (null == type || type.equals(Consumer.ConsumeType.JMQ)) {
                consumerId = message.getConsumerId();
                consumer = sessionManager.getConsumerById(consumerId.getConsumerId());
                // 判断控制读权限否
                JMQCode retCode = clusterManager.checkControllerReadable(consumer.getTopic(), consumer.getApp(), consumerId.getConnectionId().getClientId().getIp());
                if (retCode != JMQCode.SUCCESS) {
                    return CommandUtils.createBooleanAck(header.getRequestId(), retCode);
                }
            } else {
                consumer = new Consumer();
                consumer.setTopic(message.getTopic());
                consumer.setType(Consumer.ConsumeType.KAFKA);
            }

            // 获取消息
            MilliPeriod period = new MilliPeriod();
            period.begin();

            if (message.getOffset() >= 0 && message.getQueueId() <= 0) {
                throw new JMQException(String.format("offset:%d is setted,queueId:%d must be setted!", message.getOffset(), message.getQueueId()), JMQCode.CN_PARAM_ERROR.getCode());
            }
            PullResult pullResult = null;

            GetMessageAck getMessageAck = null;
            CommandCallback callback = null;
            try {
                pullResult = dispatchService.getMessage(consumer, message.getCount(), message.getAckTimeout(), message.getQueueId(), message.getOffset());
                if (pullResult != null && !pullResult.isEmpty()) {
                    period.end();
                    // 监控信息
                    brokerMonitor
                            .onGetMessage(consumer.getTopic(), consumer.getApp(), pullResult.count(), pullResult.size(),
                                    (int)period.time());
                    // 构造应答命令
                    getMessageAck = new GetMessageAck().buffers(pullResult.toArrays());
                    if (null == type || type.equals(Consumer.ConsumeType.JMQ)) {
                        // 设置回调函数，出错清理队列锁
                        callback = new GetMessageAckCallback(pullResult);
                    }
                } else if (message.getLongPull() <= 0 || !clusterManager.isNeedLongPull(message.getTopic())) {
                    // 非长轮询(longPull小于零或者顺序消息)
                    getMessageAck = new GetMessageAck();
                } else if (longPullManager.suspend(consumer, command, transport)) {
                    // 长轮询成功
                    return null;
                } else {
                    // 长轮询不成功
                    getMessageAck = new GetMessageAck();
                }

                Command response = new Command(createAckHeader(command), getMessageAck);
                if (callback != null) {
                    transport.acknowledge(command, response, callback);
                    return null;
                } else {
                    return response;
                }

            } catch (JMQException e) {
                if (pullResult != null) {
                    pullResult.release();
                }

                // 错误消息头部
                JMQHeader errHeader = createAckHeader(command);
                errHeader.setStatus(e.getCode());
                // 获取消息失败
                errHeader.setError(e.getMessage());
                logger.error(e.getMessage(), e);
                return new Command(errHeader, new GetMessageAck());
            } catch (Throwable e) {
                if (pullResult != null) {
                    pullResult.release();
                }
                throw new JMQException(e, JMQCode.CN_UNKNOWN_ERROR.getCode());

            }
        } catch (JMQException e) {
            throw new TransportException(e.getMessage(), e.getCause(),e.getCode());
        }
    }

    @Override
    public int[] type() {
        return new int[]{CmdTypes.GET_MESSAGE};
    }

    /**
     * 命令回调
     */
    protected static class GetMessageAckCallback implements CommandCallback {
        private PullResult pullResult;

        public GetMessageAckCallback(PullResult pullResult) {
            this.pullResult = pullResult;
        }

        @Override
        public void onSuccess(Command request, Command response) {

        }

        @Override
        public void onException(Command request, Throwable cause) {
            pullResult.release(false, true);
        }
    }
}