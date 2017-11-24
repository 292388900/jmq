package com.ipd.jmq.server.store;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 刷新策略
 */
public class FlushPolicy {

    // 是否同步
    private boolean sync;
    // 刷盘超时时间
    private int syncTimeout = 1000 * 5;
    // 刷盘间隔
    private int flushInterval = 1000 * 10;
    // 刷盘数据块最小大小
    private int dataSize = 4096 * 256;
    // 检查间隔
    private int checkInterval = 1000;

    public FlushPolicy() {

    }

    /**
     * 构造函数
     *
     * @param sync 是否同步刷盘
     */
    public FlushPolicy(boolean sync) {
        this.sync = sync;
    }

    /**
     * 构造函数
     *
     * @param sync          是否同步刷盘
     * @param flushInterval 时间间隔
     * @param dataSize      数据块大小
     */
    public FlushPolicy(boolean sync, int flushInterval, int dataSize) {
        this.sync = sync;
        this.flushInterval = flushInterval;
        this.dataSize = dataSize;
    }

    public boolean isSync() {
        return this.sync;
    }

    public void setSync(boolean sync) {
        this.sync = sync;
    }

    public int getSyncTimeout() {
        return this.syncTimeout;
    }

    public void setSyncTimeout(int syncTimeout) {
        this.syncTimeout = syncTimeout;
    }

    public int getFlushInterval() {
        return this.flushInterval;
    }

    public void setFlushInterval(int flushInterval) {
        this.flushInterval = flushInterval;
    }

    public int getDataSize() {
        return this.dataSize;
    }

    public void setDataSize(int dataSize) {
        this.dataSize = dataSize;
    }

    public int getCheckInterval() {
        return this.checkInterval;
    }

    public void setCheckInterval(int checkInterval) {
        this.checkInterval = checkInterval;
    }


    /**
     * 刷盘条件
     */
    public static class FlushCondition {
        // 数据大小
        private AtomicInteger size = new AtomicInteger();
        // 上次刷盘时间
        private long flushTime;

        public int getSize() {
            return size.get();
        }

        public int addSize(final int delta) {
            return size.addAndGet(delta);
        }

        public long getFlushTime() {
            return flushTime;
        }

        /**
         * 是否需要刷盘
         *
         * @param policy   刷盘策略
         * @param time     时间
         * @param onlySize 只比较数据大小
         * @return 刷盘标示
         */
        public boolean match(final FlushPolicy policy, final long time, final boolean onlySize) {
            if (policy == null) {
                return false;
            }
            if (size.get() >= policy.getDataSize() || (!onlySize && (time - flushTime) >= policy.getFlushInterval())) {
                reset(time);
                return true;
            }
            return false;
        }


        /**
         * 重置
         *
         * @param time 当前时间
         */
        public void reset(final long time) {
            size.set(0);
            flushTime = time;
        }
    }

}