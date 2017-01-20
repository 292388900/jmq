package com.ipd.jmq.common.message;

import com.ipd.jmq.common.model.JournalLog;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dingjun on 2016/5/10.
 */
public class BrokerCommit implements JournalLog{
    private int size;
    private long journalOffset;
    private long storeTime;
    private String topic;
    //事物ID
    private String txId;
    //消息条数
    private int msgCount;
    private long crc;
    private Map<Object,Object> attrs = new HashMap<Object,Object>();

    @Override
    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public long getJournalOffset() {
        return journalOffset;
    }

    public void setJournalOffset(long journalOffset) {
        this.journalOffset = journalOffset;
    }

    @Override
    public long getStoreTime() {
        return storeTime;
    }

    @Override
    public void setStoreTime(long storeTime) {
        this.storeTime = storeTime;
    }

    @Override
    public String getEndPoint() {
        return topic;
    }

    @Override
    public byte getType() {
        return JournalLog.TYPE_TX_COMMIT;
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

    public int getMsgCount() {
        return msgCount;
    }

    public void setMsgCount(int msgCount) {
        this.msgCount = msgCount;
    }

    public long getCrc() {
        return crc;
    }

    public void setCrc(long crc) {
        this.crc = crc;
    }

    public Map<Object, Object> getAttrs() {
        return attrs;
    }

    public void setAttrs(Map<Object, Object> attrs) {
        this.attrs = attrs;
    }

}
