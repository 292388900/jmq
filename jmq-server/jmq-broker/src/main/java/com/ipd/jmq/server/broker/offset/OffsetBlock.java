package com.ipd.jmq.server.broker.offset;


import com.ipd.jmq.server.broker.sequence.Sequence;
import com.ipd.jmq.server.store.ConsumeQueue;
import com.ipd.jmq.toolkit.time.SystemClock;

/**
 * Created by llw on 15-8-17.
 */
public class OffsetBlock implements Cloneable {
    //索引的开始位置
    private long minQueueOffset;
    //索引的结束位置
    private long maxQueueOffset;

    private int size;

    private long expireTime;

    private String consumerId;

    private int queueId;
    //消息是否被派发
    private boolean dispatched = false;


    public OffsetBlock(String consumerId, int queueId, long minQueueOffset, long maxQueueOffset) {

        if (minQueueOffset > maxQueueOffset) {
            throw new IllegalArgumentException("minOffset must less than maxOffset. minOffset=" + minQueueOffset + ",maxOffset=" + maxQueueOffset);
        }
        this.queueId = queueId;
        this.minQueueOffset = minQueueOffset;
        this.maxQueueOffset = maxQueueOffset;
        this.consumerId = consumerId;

        this.size = (int) ((maxQueueOffset - minQueueOffset) / ConsumeQueue.CQ_RECORD_SIZE) + 1;
    }

    public OffsetBlock(String consumerId, long minQueueOffset, long maxQueueOffset) {

        this(consumerId, (short) -1, minQueueOffset, maxQueueOffset);
    }

    public OffsetBlock(long minQueueOffset, int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("count must bigger than 0. count=" + count);
        }

        this.minQueueOffset = minQueueOffset;
        this.size = count;
        this.maxQueueOffset = minQueueOffset + ConsumeQueue.CQ_RECORD_SIZE * (count - 1);
    }


    public long getMinQueueOffset() {
        return minQueueOffset;
    }


    public long getMaxQueueOffset() {
        return maxQueueOffset;
    }


    public int size() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }


    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public Sequence toSequence() {
        return new Sequence(minQueueOffset, maxQueueOffset);
    }

    @Override
    public String toString() {
        return "OffsetBlock{" +
                "minQueueOffset=" + minQueueOffset +
                ", maxQueueOffset=" + maxQueueOffset +
                ", size=" + size +
                ", expireTime=" + expireTime +
                ", consumerId='" + consumerId + '\'' +
                ", queueId=" + queueId +
                ", dispatched=" + dispatched +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OffsetBlock block = (OffsetBlock) o;

        if (maxQueueOffset != block.maxQueueOffset) return false;
        if (minQueueOffset != block.minQueueOffset) return false;
        if (queueId != block.queueId) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (minQueueOffset ^ (minQueueOffset >>> 32));
        result = 31 * result + (int) (maxQueueOffset ^ (maxQueueOffset >>> 32));
        result = 31 * result + (int) queueId;
        return result;
    }

    public boolean expired() {
        return expireTime < SystemClock.now();
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public OffsetBlock copy() throws CloneNotSupportedException {
        return (OffsetBlock) this.clone();
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public boolean isDispatched() {
        return dispatched;
    }

    public void setDispatched(boolean dispatched) {
        this.dispatched = dispatched;
    }
}
