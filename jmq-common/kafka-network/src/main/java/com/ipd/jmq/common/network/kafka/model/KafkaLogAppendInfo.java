package com.ipd.jmq.common.network.kafka.model;

/**
 * Created by zhangkepeng on 16-8-30.
 */
public class KafkaLogAppendInfo {

    private long firstOffset;
    private long lastOffset;
    private CompressionCodec codec;
    private int shallowCount;
    private int validBytes;
    private boolean offsetsMonotonic;

    public KafkaLogAppendInfo(long firstOffset, long lastOffset, CompressionCodec codec, int shallowCount, int validBytes, boolean offsetsMonotonic) {
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.codec = codec;
        this.shallowCount = shallowCount;
        this.validBytes = validBytes;
        this.offsetsMonotonic = offsetsMonotonic;
    }

    public boolean isOffsetsMonotonic() {
        return offsetsMonotonic;
    }

    public void setOffsetsMonotonic(boolean offsetsMonotonic) {
        this.offsetsMonotonic = offsetsMonotonic;
    }

    public int getValidBytes() {
        return validBytes;
    }

    public void setValidBytes(int validBytes) {
        this.validBytes = validBytes;
    }

    public int getShallowCount() {
        return shallowCount;
    }

    public void setShallowCount(int shallowCount) {
        this.shallowCount = shallowCount;
    }

    public long getFirstOffset() {
        return firstOffset;
    }

    public void setFirstOffset(long firstOffset) {
        this.firstOffset = firstOffset;
    }

    public long getLastOffset() {
        return lastOffset;
    }

    public void setLastOffset(long lastOffset) {
        this.lastOffset = lastOffset;
    }

    public CompressionCodec getCodec() {
        return codec;
    }

    public void setCodec(CompressionCodec codec) {
        this.codec = codec;
    }
}
