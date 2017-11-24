package com.ipd.jmq.common.model;

import java.util.HashMap;
import java.util.Map;

/**
 * 生产者
 */
public class Producer extends BaseModel {
    public static final int RETRY_TIMES = 3;
    public static final int SEND_TIMEOUT = 5000;
    public static final Acknowledge ACKNOWLEDGE = Acknowledge.ACK_FLUSH;


    // 主题ID
    private Identity topic;
    // 应用代码
    private Identity app;

    public Identity getTopic() {
        return topic;
    }

    public void setTopic(Identity topic) {
        this.topic = topic;
    }

    public Identity getApp() {
        return app;
    }

    public void setApp(Identity app) {
        this.app = app;
    }

    // 重试次数
    private int retryTimes;
    // 超时时间
    private int sendTimeout;
    // 确认方式
    private Acknowledge acknowledge;
    // 是否就近发送
    private boolean nearBy;
    //集群实例发送权重
    private String weight;
    //客户端类型
    private String clientType;
    //单线程发送
    private boolean single;

    public boolean isNearBy() {
        return nearBy;
    }

    public void setNearBy(boolean nearBy) {
        this.nearBy = nearBy;
    }

    public String getWeight() {
        return weight;
    }

    public void setWeight(String weight) {
        this.weight = weight;
    }

    /**
     * 获取权重
     *
     * @return 权重
     */
    public Map<String, Short> weights() {
        if (weight == null || weight.isEmpty()) {
            return null;
        }
        Map<String, Short> map = new HashMap<String, Short>();
        String[] values = weight.split(",");
        String[] parts;
        for (String value : values) {
            parts = value.split(":");
            if (parts.length >= 2) {
                try {
                    map.put(parts[0], Short.parseShort(parts[1]));
                } catch (NumberFormatException e) {
                }
            }
        }

        return map;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public int getSendTimeout() {
        return sendTimeout;
    }

    public void setSendTimeout(int sendTimeout) {
        this.sendTimeout = sendTimeout;
    }

    public Acknowledge getAcknowledge() {
        return acknowledge;
    }

    public void setAcknowledge(Acknowledge acknowledge) {
        this.acknowledge = acknowledge;
    }

    public String getClientType() {
        return clientType;
    }

    public void setClientType(String clientType) {
        this.clientType = clientType;
    }

    public boolean isSingle() {
        return single;
    }

    public void setSingle(boolean single) {
        this.single = single;
    }
}
