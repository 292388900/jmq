package com.ipd.jmq.common.monitor;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by llw on 16-1-25.
 */
public class RetryStat {
    private String topic;
    private String app;

    //新增重试
    private AtomicLong addRetry = new AtomicLong(0);
    //消费重试成功数
    private AtomicLong retrySuccess = new AtomicLong(0);
    //消费重试失败
    private AtomicLong retryError = new AtomicLong(0);


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public AtomicLong getAddRetry() {
        return addRetry;
    }

    public void setAddRetry(AtomicLong addRetry) {
        this.addRetry = addRetry;
    }

    public AtomicLong getRetrySuccess() {
        return retrySuccess;
    }

    public void setRetrySuccess(AtomicLong retrySuccess) {
        this.retrySuccess = retrySuccess;
    }

    public AtomicLong getRetryError() {
        return retryError;
    }

    public void setRetryError(AtomicLong retryError) {
        this.retryError = retryError;
    }

    public void reset() {
        this.addRetry.set(0);
        this.retrySuccess.set(0);
        this.retryError.set(0);
    }

    public void onAddRetry(long count) {
        this.addRetry.addAndGet(count);
    }

    public void onRetrySuccess(long count) {
        this.retrySuccess.addAndGet(count);
    }

    public void onRetryError(long count) {
        this.retryError.addAndGet(count);
    }


}
