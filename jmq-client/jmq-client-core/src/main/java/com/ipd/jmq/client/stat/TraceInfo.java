package com.ipd.jmq.client.stat;

import com.ipd.jmq.toolkit.time.SystemClock;

import java.util.HashMap;
import java.util.Map;

/**
 * 方法跟踪信息
 */
public class TraceInfo {
    // 主题
    private String topic;
    // 应用
    private String app;
    // 阶段
    private TracePhase phase;
    // 开始时间
    private long startTime;
    // 结束时间
    private long endTime;
    // 消息条数
    private int count;
    // 大小
    private long size;
    // 成功标示
    private boolean success;
    // 异常
    private Throwable exception;
    // 绑定每种性能统计插件的数据对象
    private Map<Object, Object> targets = new HashMap<Object, Object>(3);

    public TraceInfo(final String topic, final String app, final TracePhase phase) {
        this(topic, app, phase, SystemClock.now());
    }

    public TraceInfo(final String topic, final String app, final TracePhase phase, final long startTime) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic can no be empty");
        }
        if (app == null || app.isEmpty()) {
            throw new IllegalArgumentException("topic can no be empty");
        }
        if (phase == null) {
            throw new IllegalArgumentException("phase can no be null");
        }
        this.topic = topic;
        this.app = app;
        this.phase = phase;
        this.startTime = startTime;
    }

    public TraceInfo count(final int count) {
        setCount(count);
        return this;
    }

    public TraceInfo size(final long size) {
        setSize(size);
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public String getApp() {
        return app;
    }

    public TracePhase getPhase() {
        return phase;
    }

    /**
     * 获取绑定的对象
     *
     * @param key 键
     * @return 值
     */
    public Object get(final Object key) {
        if (key == null) {
            return null;
        }
        return targets.get(key);
    }

    /**
     * 绑定对象
     *
     * @param key   键
     * @param value 值
     */
    public void put(final Object key, final Object value) {
        if (key != null && value != null) {
            targets.put(key, value);
        }
    }

    /**
     * 清理
     */
    public void clear() {
        startTime = 0;
        endTime = 0;
        count = 0;
        size = 0;
        targets.clear();
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public boolean isSuccess() {
        return success;
    }

    public Throwable getException() {
        return exception;
    }

    public int getElapsedTime() {
        return (int) (endTime - startTime);
    }

    /**
     * 成功调用
     */
    public TraceInfo success() {
        endTime = SystemClock.now();
        success = true;
        return this;
    }

    /**
     * 失败
     *
     * @param e 异常
     */
    public TraceInfo error(final Throwable e) {
        exception = e;
        endTime = SystemClock.now();
        success = false;
        return this;
    }

}
