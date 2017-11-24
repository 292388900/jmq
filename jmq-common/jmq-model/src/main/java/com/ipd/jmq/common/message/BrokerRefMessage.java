package com.ipd.jmq.common.message;

import com.ipd.jmq.common.model.JournalLog;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dingjun on 2016/5/10.
 */
public class BrokerRefMessage implements JournalLog{
    private int size;
    private long journalOffset;
    private long storeTime;
    private long refJournalOffset;
    private int refJournalSize;
    private short queueId;
    private long queueOffset;
    private String topic;
    private short flag;
    private long crc;
    private Map<Object,Object> attrs = new HashMap<Object,Object>();

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public byte getType() {
        return JournalLog.TYPE_REF_MESSAGE;
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

    public long getRefJournalOffset() {
        return refJournalOffset;
    }

    public void setRefJournalOffset(long refJournalOffset) {
        this.refJournalOffset = refJournalOffset;
    }

    public int getRefJournalSize() {
        return refJournalSize;
    }

    public void setRefJournalSize(int refJournalSize) {
        this.refJournalSize = refJournalSize;
    }

    public short getQueueId() {
        return queueId;
    }

    public void setQueueId(short queueId) {
        this.queueId = queueId;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public short getFlag() {
        return flag;
    }

    public void setFlag(short flag) {
        this.flag = flag;
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
