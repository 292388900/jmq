package com.ipd.jmq.server.store;

/**
 * 存储事件
 */
public class StoreEvent {
    // 类型
    private EventType type;
    // 异常
    private Throwable exception;
    private long flushedJournalOffset;

    public StoreEvent(EventType type, Throwable exception) {
        this.type = type;
        this.exception = exception;
    }

    public StoreEvent(long flushedJournalOffset) {
        this.type = EventType.FLUSHED;
        this.flushedJournalOffset = flushedJournalOffset;
    }

    public EventType getType() {
        return type;
    }

    public Throwable getException() {
        return exception;
    }

    public long getFlushedJournalOffset() {
        return flushedJournalOffset;
    }

    /**
     * 事件类型
     */
    public static enum EventType {
        /**
         * 启动
         */
        START,
        /**
         * 关闭
         */
        STOP,
        /**
         * 异常
         */
        EXCEPTION,
        /**
         * 磁盘已刷新
         */
        FLUSHED
    }
}
