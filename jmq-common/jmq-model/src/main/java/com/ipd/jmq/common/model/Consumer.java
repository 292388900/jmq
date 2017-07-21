package com.ipd.jmq.common.model;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 * 消费者
 */
public class Consumer extends BaseModel {
    // 主题ID
    private Identity topic;
    // 应用代码
    private Identity app;

    public Identity getTopic() {
        return topic;
    }

    public void setTopic(Identity topic) {
        this.topic = topic;
    }

    public Identity getApp() {
        return app;
    }

    public void setApp(Identity app) {
        this.app = app;
    }

    //
    private short version;
    // 是否就近发送
    private boolean nearby;
    // 是否暂停消费
    private boolean paused;
    // 是否需要归档,默认不归档
    private boolean archive;
    // 应答超时时间
    @Min(0)
    private int ackTimeout;
    // 批量大小
    @Min(0)
    @Max(127)
    private int batchSize;
    // 最大重试次数(无限制)
    @Min(0)
    private int maxRetrys;
    // 最大重试间隔(默认5分钟)
    @Min(0)
    private int maxRetryDelay;
    // 是否启用重试服务
    private boolean retry = true;
    // 重试间隔
    @Min(0)
    private int retryDelay;
    // 指数增加重试间隔时间
    private boolean useExpBackoff;
    // 重试指数系数
    private double backoffMultiplier;
    // 过期时间（默认3天）
    @Min(0)
    private int expireTime;
    // 延迟时间
    private int delay;
    //
    private int minConcurrent;
    //
    private int maxConcurrent;
    //
    private int longPull;
    // 指数增加间隔时间
    private int pullEmptySleep;
    // 指数系数
    @Min(0)
    private int pullTimeout;
    //单个queue并行消费
    private int concurrents;

    //客户端类型
    private short clientType;

    public OffsetMode getOffsetMode() {
        return offsetMode;
    }

    public void setOffsetMode(OffsetMode offsetMode) {
        this.offsetMode = offsetMode;
    }

    //偏移量管理类型
    private OffsetMode offsetMode;

    public boolean isNearby() {
        return nearby;
    }

    public void setNearby(boolean nearby) {
        this.nearby = nearby;
    }

    public boolean isPaused() {
        return paused;
    }

    public void setPaused(boolean paused) {
        this.paused = paused;
    }

    public boolean isArchive() {
        return archive;
    }

    public void setArchive(boolean archive) {
        this.archive = archive;
    }

    public boolean isRetry() {
        return retry;
    }

    public void setRetry(boolean retry) {
        this.retry = retry;
    }

    public int getAckTimeout() {
        return ackTimeout;
    }

    public void setAckTimeout(int ackTimeout) {
        this.ackTimeout = ackTimeout;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getMaxRetrys() {
        return maxRetrys;
    }

    public void setMaxRetrys(int maxRetrys) {
        this.maxRetrys = maxRetrys;
    }

    public int getMaxRetryDelay() {
        return maxRetryDelay;
    }

    public void setMaxRetryDelay(int maxRetryDelay) {
        this.maxRetryDelay = maxRetryDelay;
    }

    public int getRetryDelay() {
        return retryDelay;
    }

    public void setRetryDelay(int retryDelay) {
        this.retryDelay = retryDelay;
    }

    public int getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(int expireTime) {
        this.expireTime = expireTime;
    }

    public int getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        this.delay = delay;
    }

    public short getVersion() {
        return version;
    }

    public void setVersion(short version) {
        this.version = version;
    }

    public boolean isUseExpBackoff() {
        return useExpBackoff;
    }

    public void setUseExpBackoff(boolean useExpBackoff) {
        this.useExpBackoff = useExpBackoff;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void setBackoffMultiplier(double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }

    public int getMinConcurrent() {
        return minConcurrent;
    }

    public void setMinConcurrent(int minConcurrent) {
        this.minConcurrent = minConcurrent;
    }

    public int getMaxConcurrent() {
        return maxConcurrent;
    }

    public void setMaxConcurrent(int maxConcurrent) {
        this.maxConcurrent = maxConcurrent;
    }

    public int getLongPull() {
        return longPull;
    }

    public void setLongPull(int longPull) {
        this.longPull = longPull;
    }

    public int getPullEmptySleep() {
        return pullEmptySleep;
    }

    public void setPullEmptySleep(int pullEmptySleep) {
        this.pullEmptySleep = pullEmptySleep;
    }

    public int getPullTimeout() {
        return pullTimeout;
    }

    public void setPullTimeout(int pullTimeout) {
        this.pullTimeout = pullTimeout;
    }

    public int getConcurrents() {
        return concurrents;
    }

    public void setConcurrents(int concurrents) {
        this.concurrents = concurrents;
    }

    public short getClientType() {
        return clientType;
    }

    public void setClientType(short clientType) {
        this.clientType = clientType;
    }
}
