package com.ipd.jmq.common.message;

import com.ipd.jmq.common.model.JournalLog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author dingjun
 * @since 2016/5/10.
 */
public class BrokerPrepare implements JournalLog {
    private int size;
    //日志写入位置
    private long journalOffset;
    //存储时间
    private long storeTime;
    //主题
    private String topic;
    //事物ID
    private String txId;
    //事物查询标识
    private String queryId;
    //校验码
    private long crc;
    //开始时间
    private long startTime;
    //超时时间
    private int timeout;
    //超时后执行的动作
    private byte timeoutAction;
    private Map<Object, Object> attrs = new HashMap<Object, Object>();
    private List<BrokerRefMessage> messages = new ArrayList<BrokerRefMessage>();

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
        return JournalLog.TYPE_TX_PREPARE;
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

    public List<BrokerRefMessage> getMessages() {
        return messages;
    }

    public void setMessages(List<BrokerRefMessage> messages) {
        this.messages = messages;
    }

    public void addMessage(BrokerRefMessage message) {
        messages.add(message);
    }

    public void addMessages(List<BrokerRefMessage> messageList) {
        messages.addAll(messageList);
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }


    public byte getTimeoutAction() {
        return timeoutAction;
    }

    public void setTimeoutAction(byte timeoutAction) {
        this.timeoutAction = timeoutAction;
    }

    @Override
    public String toString() {
        return "BrokerPrepare{" +
                "size=" + size +
                ", journalOffset=" + journalOffset +
                ", storeTime=" + storeTime +
                ", type=" + getType() +
                ", topic='" + topic + '\'' +
                ", txId='" + txId + '\'' +
                ", queryId='" + queryId + '\'' +
                ", attrs=" + attrs +
                ", timeout=" + timeout +
                ", timeoutAction=" + timeoutAction +
                '}';
    }
}
