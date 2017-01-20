package com.ipd.jmq.server.store;

import com.ipd.jmq.common.network.v3.command.TxStatus;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lindeqiang
 * @since 2016/5/17 18:13
 */
public class GetTxResult {
    private TxStatus status;
    private String topic;
    private String txId;
    private String queryId;
    private long startTime;
    private long storeTime;
    private int msgCount;

    public GetTxResult() {
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getStoreTime() {
        return storeTime;
    }

    public void setStoreTime(long storeTime) {
        this.storeTime = storeTime;
    }

    public int getMsgCount() {
        return msgCount;
    }

    public void setMsgCount(int msgCount) {
        this.msgCount = msgCount;
    }

    public TxStatus getStatus() {
        return status;
    }

    public void setStatus(TxStatus status) {
        this.status = status;
    }

}
