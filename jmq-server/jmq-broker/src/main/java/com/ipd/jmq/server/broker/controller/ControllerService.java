package com.ipd.jmq.server.broker.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by zhangkepeng on 16-5-18.
 */
public class ControllerService {

    private static final Logger logger = LoggerFactory.getLogger(ControllerService.class);

    // 主题级别控制策略
    private Map<String, ControllerPolicy> topicControllerPolicyMap;
    // 应用级别控制策略
    private Map<String, ControllerPolicy> appControllerPolicyMap;
    // 分组级别控制策略
    private Map<String, ControllerPolicy> groupControllerPolicyMap;

    public ControllerService(Map<String, ControllerPolicy> topicControllerPolicyMap, Map<String, ControllerPolicy> appControllerPolicyMap) {
        this(topicControllerPolicyMap, appControllerPolicyMap, null);
    }

    public ControllerService(Map<String, ControllerPolicy> topicControllerPolicyMap, Map<String, ControllerPolicy> appControllerPolicyMap, Map<String, ControllerPolicy> groupControllerPolicyMap) {
        this.topicControllerPolicyMap = topicControllerPolicyMap;
        this.appControllerPolicyMap = appControllerPolicyMap;
        this.groupControllerPolicyMap = groupControllerPolicyMap;
    }

    /**
     * 判断应用是否设置某个IP限制生产或消费
     *
     * @param topic
     * @param app
     * @param address
     * @param type
     * @return
     */
    public boolean appClientIPLimit(String topic, String app, String address, boolean type) {
        if (appControllerPolicyMap != null && !appControllerPolicyMap.isEmpty()) {
            ControllerPolicy appClientIPLimitControllerPolicy = appControllerPolicyMap.get(topic + "@" +app);
            if (null != appClientIPLimitControllerPolicy) {
                if (address != null) {
                    boolean isAppClientIPLimit = appClientIPLimitControllerPolicy.appClientIPLimit(address, type);
                    // 客户端对IP做了限制
                    if (isAppClientIPLimit) {
                        // 控制服务端日志输出
                        if (type) {
                            ControllerPrintLog.printLog(app, ControllerType.APP_CLIENT_IP_LIMIT, ControllerPrintLog.APP_LEVEL_PRODUCE_PRINT_LOG, ControllerPrintLog.APP_LIMIT_IP_PRODUCE);
                        }else {
                            ControllerPrintLog.printLog(app, ControllerType.APP_CLIENT_IP_LIMIT, ControllerPrintLog.APP_LEVEL_CONSUME_PRINT_LOG, ControllerPrintLog.APP_LIMIT_IP_CONSUME);
                        }
                    }
                    return isAppClientIPLimit;
                }
            }
        }
        return false;
    }

    /**
     * 判断某个topic是否限制某个分片的权限
     *
     * @param topic
     * @param group
     * @param type
     * @return
     */
    public boolean topicBrokerPermission(String topic, String group, boolean type) {
        if (topicControllerPolicyMap != null && !topicControllerPolicyMap.isEmpty()) {
            ControllerPolicy topicBrokerControllerPolicy = topicControllerPolicyMap.get(topic);
            if (null != topicBrokerControllerPolicy) {
                boolean isLimitedBroker = topicBrokerControllerPolicy.topicBrokerPermission(group, type);
                if (isLimitedBroker) {
                    // 控制服务端日志输出
                    if (type) {
                        ControllerPrintLog.printLog(topic, ControllerType.TOPIC_BROKER_PERMISSION, ControllerPrintLog.TOPIC_LEVEL_PRODUCE_PRINT_LOG, ControllerPrintLog.TOPIC_BROKER_LIMIT_WRITE);
                    } else {
                        ControllerPrintLog.printLog(topic, ControllerType.TOPIC_BROKER_PERMISSION, ControllerPrintLog.TOPIC_LEVEL_CONSUME_PRINT_LOG, ControllerPrintLog.TOPIC_BROKER_LIMIT_READ);
                    }
                }
                return isLimitedBroker;
            }
        }
        return false;
    }
}