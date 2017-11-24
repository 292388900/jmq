package com.ipd.jmq.server.broker.dispatch;


import com.ipd.jmq.toolkit.time.SystemClock;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by dingjun on 15-8-10.
 */
public class ConsumerErrStat {
    private AtomicInteger count = new AtomicInteger(0);

    private AtomicInteger expiredCount = new AtomicInteger(0);

    private String consumerId;

    private long timestamp;

    private long pauseTimestamp;

    private long lastTimestamp;

    private AtomicInteger pause = new AtomicInteger(0);

    private AtomicBoolean needThrowErr = new AtomicBoolean(true);

    public ConsumerErrStat(String consumerId) {
        this.consumerId = consumerId;
        timestamp = SystemClock.now();
        pauseTimestamp = timestamp;
        lastTimestamp = timestamp;
    }

    public int getCount() {
        return count.get();
    }

    public int getExpired(){
        return expiredCount.get();
    }

    private void setCount(AtomicInteger count) {
        this.count = count;
    }

    public int incrementErr(){
        lastTimestamp = SystemClock.now();
        return count.incrementAndGet();
    }

    public int incrementExpired(){
        lastTimestamp = SystemClock.now();
        return expiredCount.incrementAndGet();
    }

    public int incrementPause(){
        int i = pause.incrementAndGet();
        if(i == 1){
            pauseTimestamp = SystemClock.now();
        }
        return i;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }

    public void setLastTimestamp(long lastTimestamp) {
        this.lastTimestamp = lastTimestamp;
    }

    public long getPauseTimestamp() {
        return pauseTimestamp;
    }

    public void setPauseTimestamp(long pauseTimestamp) {
        this.pauseTimestamp = pauseTimestamp;
    }

    public int getPause() {
        return pause.get();
    }

    private void setPause(AtomicInteger pause) {
        this.pause = pause;
    }

    public boolean getNeedThrowErr() {
        return needThrowErr.get();
    }

    public boolean setNeedThrowErr(boolean needThrowErr) {
        return this.needThrowErr.compareAndSet(!needThrowErr,needThrowErr);
    }

    @Override
    public String toString() {
        return "ConsumerErrStat{" +
                "count=" + count +
                ", expiredCount=" + expiredCount +
                ", consumerId='" + consumerId + '\'' +
                ", timestamp=" + timestamp +
                ", pauseTimestamp=" + pauseTimestamp +
                ", lastTimestamp=" + lastTimestamp +
                ", pause=" + pause +
                ", needThrowErr=" + needThrowErr +
                '}';
    }
}
