package com.ipd.jmq.common.model;

import com.ipd.jmq.common.message.BrokerMessage;

/**
 * Created by zhangkepeng on 16-1-6.
 */
public class BrokerMessageInfo extends BrokerMessage {

    private String id;

    private String consumerApp;

    private long createTime;

    private byte[] brokerMsgBytes;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public String getConsumerApp() {
        return consumerApp;
    }

    public void setConsumerApp(String consumerApp) {
        this.consumerApp = consumerApp;
    }

    public byte[] getBrokerMsgBytes() {
        return brokerMsgBytes;
    }

    public void setBrokerMsgBytes(byte[] brokerMsgBytes) {
        this.brokerMsgBytes = brokerMsgBytes;
    }
}
