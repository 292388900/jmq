package com.ipd.jmq.common.cluster;


import com.ipd.jmq.toolkit.time.SystemClock;

/**
 * 顺序消息消费状态.
 * <p/>
 * 记录当前应用可读的Broker列表
 *
 * @author lindeqiang
 * @since 2015/8/18 16:09
 */
public class AppConsumeState {
    //应用
    private String app;
    //时间戳
    private long timestamp = SystemClock.now();
    //消费者消费状态
    private SequentialBrokerState brokerState = new SequentialBrokerState();

    public AppConsumeState() {
        // noting to do
    }

    public AppConsumeState(String app) {
        this.app = app;
    }

    /**
     * 添加可写的Broker
     *
     * @param sequentialBroker 新分配的可写的Broker
     */
    public void addWritableBroker(SequentialBrokerState.SequentialBroker sequentialBroker) {
        brokerState.addWritableBroker(sequentialBroker);
    }


    /**
     * 更新消费状态,主要是是否有积压
     *
     * @param backlog 是否积压消息
     * @param group   Broker分组
     * @return 是否有更改
     */
    public boolean updateState(boolean backlog, String group) {
        boolean changed = false;
        if (!backlog) {
            changed = brokerState.removeIdledGroup(new SequentialBrokerState.SequentialBroker(group));
        }
        timestamp = SystemClock.now();
        return changed;
    }

    /**
     * 获取可读的Broker
     *
     * @return 可读的Broker
     */
    public SequentialBrokerState.SequentialBroker fetchReadableBroker() {
        return brokerState.fetchReadableBroker();
    }


    public AppConsumeState clone() {
        AppConsumeState copy = new AppConsumeState(this.app);
        copy.setBrokerState(brokerState.clone());
        copy.setTimestamp(this.timestamp);
        return copy;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public SequentialBrokerState getBrokerState() {
        return brokerState;
    }

    public void setBrokerState(SequentialBrokerState brokerState) {
        this.brokerState = brokerState;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "AppConsumeState{" +
                "app='" + app + '\'' +
                ", brokerState=" + brokerState +
                '}';
    }
}
