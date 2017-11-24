package com.ipd.jmq.common.monitor;

import java.io.Serializable;

/**
 * Created by llw on 16-1-21.
 */
public class MetricsInfo implements Serializable {

    public static final String PRODUCER = "p";
    public static final String CONSUMER = "c";


    private String topic;
    private String app;
    private String role;

    //生产者监控参数
    private long pMax;
    private long pTp99;
    private long pTp50;
    private long pTps;

    //消费者监控参数
    private long cMax;
    private long cTp99;
    private long cTp50;
    private long cTps;

    //重试统计
    //新增重试
    private long addRetry;
    //消费重试成功数
    private long retrySuccess;
    //消费重试失败
    private long retryError;


    public boolean isProducer(){
        return PRODUCER.equals(this.role);
    }

    public boolean isConsumer(){
        return CONSUMER.equals(this.role);
    }

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


    public long getpMax() {
        return pMax;
    }

    public void setpMax(long pMax) {
        this.pMax = pMax;
    }

    public long getpTp99() {
        return pTp99;
    }

    public void setpTp99(long pTp99) {
        this.pTp99 = pTp99;
    }

    public long getpTp50() {
        return pTp50;
    }

    public void setpTp50(long pTp50) {
        this.pTp50 = pTp50;
    }

    public long getpTps() {
        return pTps;
    }

    public void setpTps(long pTps) {
        this.pTps = pTps;
    }

    public long getcMax() {
        return cMax;
    }

    public void setcMax(long cMax) {
        this.cMax = cMax;
    }

    public long getcTp99() {
        return cTp99;
    }

    public void setcTp99(long cTp99) {
        this.cTp99 = cTp99;
    }

    public long getcTp50() {
        return cTp50;
    }

    public void setcTp50(long cTp50) {
        this.cTp50 = cTp50;
    }

    public long getcTps() {
        return cTps;
    }

    public void setcTps(long cTps) {
        this.cTps = cTps;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public long getAddRetry() {
        return addRetry;
    }

    public void setAddRetry(long addRetry) {
        this.addRetry = addRetry;
    }

    public long getRetrySuccess() {
        return retrySuccess;
    }

    public void setRetrySuccess(long retrySuccess) {
        this.retrySuccess = retrySuccess;
    }

    public long getRetryError() {
        return retryError;
    }

    public void setRetryError(long retryError) {
        this.retryError = retryError;
    }
}
