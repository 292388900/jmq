package com.ipd.jmq.server.broker.handler.telnet;

import com.alibaba.fastjson.JSONObject;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.cluster.Permission;
import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.network.command.telnet.Commands;
import com.ipd.jmq.common.network.command.telnet.TelnetCode;
import com.ipd.jmq.common.network.command.telnet.TelnetResult;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.common.network.v3.netty.telnet.param.PermiQueryParam;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.common.network.protocol.telnet.TelnetHandler;
import com.ipd.jmq.common.network.protocol.telnet.TelnetHeader;
import com.ipd.jmq.common.network.protocol.telnet.TelnetRequest;
import com.ipd.jmq.common.network.protocol.telnet.TelnetResponse;
import com.ipd.jmq.toolkit.lang.Preconditions;

import java.util.Map;
import java.util.Set;

/**
 * Created by zhangkepeng on 16-12-12.
 */
public class PermiQueryHandler implements TelnetHandler<Transport> {

    private ClusterManager clusterManager;

    public PermiQueryHandler(ClusterManager clusterManager) {
        Preconditions.checkArgument(clusterManager != null, "clusterManager can not be null");
        this.clusterManager = clusterManager;
    }

    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {
        String text = null;
        short status = TelnetCode.NO_ERROR;
        TelnetResult telnetResult = new TelnetResult();
        TelnetRequest payload = (TelnetRequest) command.getPayload();
        String args[] = payload.getArgs();
        PermiQueryParam permiQueryParam = new PermiQueryParam();
        try {
            new JCommander(permiQueryParam, args);
        } catch (ParameterException e) {
            // 命令参数异常，进行异常处理
            status = TelnetCode.PARAM_ERROR;
            text = TelnetCode.ERROR_TEXT.get(TelnetCode.PARAM_ERROR);
            telnetResult.setMessage(text);
            telnetResult.setStatus(status);
            return new Command(TelnetHeader.Builder.response(), telnetResult);
        }

        boolean isControllerReadable = false;
        boolean isControllerWritable = false;
        boolean isBrokerReadable = false;
        boolean isBrokerWritable = false;
        boolean isReadable = false;
        boolean isWritable = false;
        String level = permiQueryParam.getLevel();
        Permission permission = Permission.FULL;
        if (level == null) {
            status = TelnetCode.PARAM_ERROR;
            text = TelnetCode.ERROR_TEXT.get(TelnetCode.PARAM_ERROR);
        } else {
            if (level.equals(PermiQueryParam.Level.BROKER.getLevel())) {
                Broker localBroker = clusterManager.getBroker();
                if (localBroker == null) {
                    text = TelnetCode.ERROR_TEXT.get(TelnetCode.NO_LOCAL_BROKER_EXCEPTION);
                    status = TelnetCode.NO_LOCAL_BROKER_EXCEPTION;
                } else {
                    Permission localPermission = localBroker.getPermission();
                    text = JSONObject.toJSONString(localPermission);
                }
            } else if (level.equals(PermiQueryParam.Level.TOPIC.getLevel())) {
                String topic = permiQueryParam.getTopic();
                if (topic == null) {
                    text = TelnetCode.ERROR_TEXT.get(TelnetCode.PARAM_ERROR);
                    status = TelnetCode.PARAM_ERROR;
                } else {
                    TopicConfig topicConfig = clusterManager.getTopicConfig(topic);
                    if (topicConfig != null) {
                        Map<String, TopicConfig.ConsumerPolicy> consumerPolicyMap = topicConfig.getConsumers();
                        Map<String, TopicConfig.ProducerPolicy> producerPolicyMap = topicConfig.getProducers();
                        Set<String> consumers = consumerPolicyMap.keySet();
                        Set<String> producers = producerPolicyMap.keySet();
                        // 验证是否有读权限
                        for (String consumer : consumers) {
                            JMQCode code = clusterManager.checkControllerReadable(topic, consumer, null);
                            if (!code.equals(JMQCode.FW_GET_MESSAGE_TOPIC_NOT_READ)) {
                                isControllerReadable = true;
                            }
                            try {
                                clusterManager.checkReadable(topic, consumer);
                                isBrokerReadable = true;
                            } catch (JMQException e) {
                                // do nothing
                            }
                            if (isControllerReadable && isBrokerReadable) {
                                isReadable = true;
                                break;
                            }
                            isControllerReadable = false;
                            isBrokerReadable = false;
                        }
                        // 该主题下没有消费者，就没有读权限
                        if (!isReadable || consumers == null || consumers.isEmpty()) {
                            permission = permission.removeRead();
                        }

                        // 验证是否有写权限
                        for (String producer : producers) {
                            JMQCode code = clusterManager.checkControllerWritable(topic, producer, null);
                            if (!code.equals(JMQCode.FW_PUT_MESSAGE_TOPIC_NOT_WRITE)) {
                                isControllerWritable = true;
                            }
                            try {
                                clusterManager.checkWritable(topic, producer);
                                isBrokerWritable = true;
                            } catch (JMQException e) {
                                // do nothing
                            }
                            if (isControllerWritable && isBrokerWritable) {
                                isWritable = true;
                                break;
                            }
                            isControllerWritable = false;
                            isBrokerWritable = false;
                        }
                        // 该主题下没有生产者，就没有写权限
                        if (!isWritable || producers == null || producers.isEmpty()) {
                            permission = permission.removeWrite();
                        }
                        text = JSONObject.toJSONString(permission);
                    } else {
                        // 没有分配，权限为none
                        permission = Permission.NONE;
                        text = JSONObject.toJSONString(permission);
                    }
                }
            } else if (level.equals(PermiQueryParam.Level.APP.getLevel())) {
                String topic = permiQueryParam.getTopic();
                String app = permiQueryParam.getApp();
                if (topic == null || app == null) {
                    text = TelnetCode.ERROR_TEXT.get(TelnetCode.PARAM_ERROR);
                    status = TelnetCode.PARAM_ERROR;
                } else {
                    // 验证控制读权限
                    JMQCode readableCode = clusterManager.checkControllerReadable(topic, app, null);
                    if (!readableCode.equals(JMQCode.FW_GET_MESSAGE_TOPIC_NOT_READ)) {
                        isControllerReadable = true;
                    }
                    // 验证读权限
                    try {
                        clusterManager.checkReadable(topic, app);
                        isBrokerReadable = true;
                    } catch (JMQException e) {
                        // do nothing
                    }
                    if (isControllerReadable && isBrokerReadable) {
                        isReadable = true;
                    }
                    if (!isReadable) {
                        permission.removeRead();
                    }
                    // 验证控制写权限
                    JMQCode writableCode = clusterManager.checkControllerWritable(topic, app, null);
                    if (!writableCode.equals(JMQCode.FW_GET_MESSAGE_TOPIC_NOT_READ)) {
                        isControllerWritable = true;
                    }
                    // 验证写权限
                    try {
                        clusterManager.checkWritable(topic, app);
                        isBrokerWritable = true;
                    } catch (JMQException e) {
                        // do nothing
                    }
                    if (isControllerWritable && isBrokerWritable) {
                        isWritable = true;
                    }
                    if (!isWritable) {
                        permission.removeWrite();
                    }
                    text = JSONObject.toJSONString(permission);
                }
            } else {
                text = TelnetCode.ERROR_TEXT.get(TelnetCode.PARAM_ERROR);
                status = TelnetCode.PARAM_ERROR;
            }
        }

        telnetResult.setMessage(text);
        telnetResult.setStatus(status);
        telnetResult.setType(level);
        return new Command(TelnetHeader.Builder.response(),
                new TelnetResponse(JSONObject.toJSONString(telnetResult), true,true));
    }

    /**
     * 命令
     *
     * @return
     */
    @Override
    public String command() {
        return Commands.PERMIQUERY;
    }

    /**
     * 帮助信息
     *
     * @return 帮助信息
     */
    @Override
    public String help() {
        return null;
    }
}
