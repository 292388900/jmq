package com.ipd.jmq.server.broker.offset;

import com.ipd.jmq.server.broker.sequence.Sequence;
import com.ipd.jmq.server.broker.sequence.SequenceSet;
import com.ipd.jmq.server.store.ConsumeQueue;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 非顺序消费偏移量
 *
 * 保存非连续的确认位置
 * @see #
 *
 *
 */
public class UnSequenceOffset extends Offset {
    //拉取最大位置,用于非顺序消费
    private transient AtomicLong pullEndOffset = new AtomicLong(0);
    private SequenceSet acks = new SequenceSet();

    public UnSequenceOffset() {
    }

    public UnSequenceOffset(String topic, int queueId, String consumer) {
        super(topic, queueId, consumer);
    }

    public SequenceSet getAcks() {
        return acks;
    }

    public void setAcks(SequenceSet acks) {
        this.acks = acks;
    }

    public void cleanAcks(){
        this.acks = new SequenceSet();
    }

    public AtomicLong getPullEndOffset() {
        return pullEndOffset;
    }

    public void setPullEndOffset(AtomicLong pullEndOffset) {
        this.pullEndOffset = pullEndOffset;
    }

    public void updateOffset(Offset offset) {
        // 保证ackOffset与acks一至
        if (offset.getAckOffset().get() <= getAckOffset().get()) {
            return;
        }

        super.updateOffset(offset);
        SequenceSet set = new SequenceSet();
        long ackOffset = getAckOffset().get();

        if (offset instanceof UnSequenceOffset) {
            UnSequenceOffset unSeqOffset = (UnSequenceOffset) offset;
            set = unSeqOffset.getAcks();
        } else if (ackOffset > 0) {
            long startOffset = getSubscribeOffset().get() + ConsumeQueue.CQ_RECORD_SIZE;
            if (startOffset <= ackOffset) {
                set.addFirst(new Sequence(startOffset, ackOffset));
            }
        }
        setAcks(set);
    }

    @Override
    public UnSequenceOffset clone() throws CloneNotSupportedException {
        UnSequenceOffset copyOfUnSequenceOffset = (UnSequenceOffset) super.clone();
        copyOfUnSequenceOffset.acks = new SequenceSet(acks);
        return copyOfUnSequenceOffset;
    }
}