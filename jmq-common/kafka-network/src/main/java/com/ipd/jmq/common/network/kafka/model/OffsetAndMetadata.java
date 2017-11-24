package com.ipd.jmq.common.network.kafka.model;

/**
 * Created by zhangkepeng on 16-7-28.
 */
public class OffsetAndMetadata {

    public static final long INVALID_OFFSET = -1L;
    public static final String NO_METADATA = "";

    public long offsetCacheRetainTime;

    private long offset;
    private String metadata;

    public OffsetAndMetadata() {

    }

    public OffsetAndMetadata(long offset, String metadata) {
        this.offset = offset;
        this.metadata = metadata;
    }

    public long getOffsetCacheRetainTime() {
        return offsetCacheRetainTime;
    }

    public void setOffsetCacheRetainTime(long offsetCacheRetainTime) {
        this.offsetCacheRetainTime = offsetCacheRetainTime;
    }

    public long getOffset() {
        return offset;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }
}
