package com.ipd.jmq.common.model;


/**
 * 生产者配置
 */
public class ProducerConfig {
    // 重试次数
    private int retryTimes = 2; // dynamic
    // send timeout for netty
    private int sendTimeout = -1; // dynamic
    // 确认方式
    private Acknowledge acknowledge = Acknowledge.ACK_RECEIVE; // dynamic

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
        if (acknowledge != null) {
            this.acknowledge = acknowledge;
        }
    }
}