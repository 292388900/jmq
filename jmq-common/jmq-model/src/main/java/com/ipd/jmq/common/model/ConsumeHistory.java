package com.ipd.jmq.common.model;

import java.util.Date;

/**
 * 归档消费历史
 */
public class ConsumeHistory implements ArchiveMessage {
    // 消费历史id
	private String id;
    // 消息id
    private String messageId;
    // 消息主题
    private String topic;
    // 消费者
    private String app;
    // 消费时间
    private long consumeTime;
    // 消费者IP
    private String clientIp;
    // 创建时间
    private long createTime;
    // 日志偏移量
    private long journalOffset;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public Date getArchiveTime() {
        return new Date(createTime);
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public long getConsumeTime() {
        return consumeTime;
    }

    public void setConsumeTime(long consumeTime) {
        this.consumeTime = consumeTime;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getJournalOffset() {
        return journalOffset;
    }

    public void setJournalOffset(long journalOffset) {
        this.journalOffset = journalOffset;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ConsumeHistory{");
        sb.append("messageId='").append(messageId).append('\'');
        sb.append(", topic='").append(topic).append('\'');
        sb.append(", app='").append(app).append('\'');
        sb.append(", consumeTime=").append(consumeTime);
        sb.append(", clientIp='").append(clientIp).append('\'');
        sb.append(", createTime=").append(createTime);
        sb.append(", journalOffset=").append(journalOffset);
        sb.append('}');
        return sb.toString();
    }
}
