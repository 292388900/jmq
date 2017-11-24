package com.ipd.jmq.server.store;

import com.ipd.jmq.common.exception.JMQException;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * 增量复制
 *
 * @author tianya
 * @since 2014-04-24
 */
public interface Replication {

    /**
     * 复制数据
     *
     * @param event 数据
     * @throws JMQException
     */
    void replicate(ReplicationEvent event) throws JMQException;

    /**
     * 复制事件
     */
    public static class ReplicationEvent {
        // 数据偏移量
        private long offset;
        // 数据
        private ByteBuffer buffer;
        // 是否完成
        private AtomicBoolean finished = new AtomicBoolean(false);
        // 是否成功
        private AtomicBoolean success = new AtomicBoolean(false);
        // 创建时间，用于超时判断
        private long createTime;
        // 异常
        private Exception exception;
        // 栅栏
        private CountDownLatch latch;

        // 开始时间(纳秒)
        private long statCreateTime;
        // 结束时间(纳秒)
        private long statFinishTime;
        // 是否开始时间统计
        private boolean stat;

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public long getOffset() {
            return this.offset;
        }

        public long getNextOffset() {
            return offset + buffer.remaining();
        }

        public void setBuffer(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        public ByteBuffer getBuffer() {
            return this.buffer;
        }

        public boolean isSuccess() {
            return success.get();
        }

        public void setSuccess(boolean isSuccess) {
            this.success.set(isSuccess);
        }

        public long getCreateTime() {
            return createTime;
        }

        public void setCreateTime(long createTime) {
            this.createTime = createTime;
        }

        public Exception getException() {
            return exception;
        }

        public void setException(Exception e) {
            this.exception = e;
        }

        public CountDownLatch getLatch() {
            return latch;
        }

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        public long getStatCreateTime() {
            return statCreateTime;
        }

        public void setStatCreateTime() {
            if (stat) {
                this.statCreateTime = System.nanoTime();
            }
        }

        public long getStatFinishTime() {
            return statFinishTime;
        }

        public void setStatFinishTime() {
            if (stat) {
                this.statFinishTime = System.nanoTime();
            }
        }

        public boolean isStat() {
            return stat;
        }

        public void setStat(boolean stat) {
            this.stat = stat;
        }

        /**
         * 完成
         *
         * @return
         */
        public boolean done() {
            setStatFinishTime();
            return finished.compareAndSet(false, true);
        }

        /**
         * 是否完成
         *
         * @return
         */
        public boolean isDone() {
            return finished.get();
        }

        /**
         * 成功
         */
        public void success() {
            setSuccess(true);
            if (done()) {
                countDown();
            }
        }

        /**
         * 失败
         *
         * @param e 异常
         */
        public void fail(JMQException e) {
            setException(e);
            setSuccess(false);
            if (done()) {
                countDown();
            }
        }

        /**
         * 减少计数
         */
        public void countDown() {
            if (latch != null) {
                latch.countDown();
            }
        }

        /**
         * 重置，可以复用该对象
         */
        public void reset() {
            this.offset = 0;
            this.buffer = null;
            this.finished.set(false);
            this.success.set(false);
            this.exception = null;
        }

    }
}