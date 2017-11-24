package com.ipd.jmq.server.store;

import com.ipd.jmq.server.store.journal.CommitRequest;
import com.ipd.jmq.toolkit.time.NanoPeriod;
import com.ipd.jmq.toolkit.time.Period;

import java.util.concurrent.CountDownLatch;

/**
 * Created by dingjun on 2016/5/9.
 */
public class FlushRequest implements CommitRequest {
    public FlushRequest() {
    }
    protected CountDownLatch latch;
    protected long offset;
    protected int size;
    protected NanoPeriod period = new NanoPeriod();

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public long getOffset() {
        return offset;
    }

    @Override
    public Period getWrite() {
        return period;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public CountDownLatch getLatch() {
        return latch;
    }

    @Override
    public void setException(Throwable throwable) {

    }
}
