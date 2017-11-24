package com.ipd.jmq.server.store;

import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.message.BrokerRefMessage;
import com.ipd.jmq.common.message.QueueItem;

/**
 * 派发请求
 */
public class DispatchRequest extends QueueItem {
    // 业务ID
    protected String businessId;
    // 优先级
    protected byte priority;
    // 顺序消息
    protected boolean ordered;
    // 恢复重试标示
    protected boolean isRetry;

    public DispatchRequest() {
    }

    public DispatchRequest(final BrokerRefMessage message, int size, final boolean isRetry, final boolean isStat) {
        setTopic(message.getTopic());
        setQueueId(message.getQueueId());
        setJournalOffset(message.getJournalOffset());
        setSize(size);
        setQueueOffset(message.getQueueOffset());
        setFlag(message.getFlag());
        setRetry(isRetry);
    }

    public DispatchRequest(final BrokerMessage message) {
        this(message, false, false);
    }

    public DispatchRequest(final boolean isStat) {
        this(null, false, isStat);
    }

    public DispatchRequest(final BrokerMessage message, final boolean isStat) {
        this(message, false, isStat);
    }

    public DispatchRequest(final BrokerMessage message, final boolean isRetry, final boolean isStat) {
        if (message != null) {
            setTopic(message.getTopic());
            setQueueId(message.getQueueId());
            setJournalOffset(message.getJournalOffset());
            setSize(message.getSize());
            setQueueOffset(message.getQueueOffset());
            setFlag(message.getFlag());
            setBusinessId(message.getBusinessId());
            setOrdered(message.isOrdered());
            setPriority(message.getPriority());
        }
        setRetry(isRetry);
    }

    public String getBusinessId() {
        return businessId;
    }

    public void setBusinessId(String businessId) {
        this.businessId = businessId;
    }

    public byte getPriority() {
        return priority;
    }

    public void setPriority(byte priority) {
        this.priority = priority;
    }

    public boolean isOrdered() {
        return ordered;
    }

    public void setOrdered(boolean ordered) {
        this.ordered = ordered;
    }

    public boolean isRetry() {
        return isRetry;
    }

    public void setRetry(boolean isRetry) {
        this.isRetry = isRetry;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DispatchRequest{");
        sb.append("topic='").append(topic).append('\'');
        sb.append(", queueId=").append(queueId);
        sb.append(", queueOffset=").append(queueOffset);
        sb.append(", journalOffset=").append(journalOffset);
        sb.append(", size=").append(size);
        sb.append(", flag=").append(flag);
        sb.append(", businessId='").append(businessId).append('\'');
        sb.append(", priority=").append(priority);
        sb.append(", ordered=").append(ordered);
        sb.append(", isRetry=").append(isRetry);
        sb.append('}');
        return sb.toString();
    }

}
