package com.ipd.jmq.server.store;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lindeqiang
 * @since 2016/7/27 21:26
 */
public class MinTimeOffset {
    private long offset;
    private long timeStamp;

    public MinTimeOffset(long offset, long timeStamp) {
        this.offset = offset;
        this.timeStamp = timeStamp;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }
}
