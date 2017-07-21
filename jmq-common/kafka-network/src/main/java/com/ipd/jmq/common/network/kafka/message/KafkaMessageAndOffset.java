package com.ipd.jmq.common.network.kafka.message;

/**
 * Created by zhangkepeng on 16-8-31.
 */
public class KafkaMessageAndOffset {

    private KafkaMessage message;
    private long offset;

    public KafkaMessageAndOffset(KafkaMessage message, long offset) {
        this.message = message;
        this.offset = offset;
    }

    public KafkaMessage getMessage() {
        return message;
    }

    public void setMessage(KafkaMessage message) {
        this.message = message;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long nextOffset() {
        return offset += 22;
    }
}
