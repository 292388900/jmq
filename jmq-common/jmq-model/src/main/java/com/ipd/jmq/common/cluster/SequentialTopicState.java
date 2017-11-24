package com.ipd.jmq.common.cluster;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 顺序消息状态
 * <p/>
 * 维护当前主题可写的Broker列表，以及订阅此主题的所有APP可读的Broker列表.并提供相关操作
 *
 * @author lindeqiang
 * @since 2015/8/17 17:26
 */
public class SequentialTopicState {
    // 消费状态列表长度
    private static final int SEQUENTIAL_APP_SIZE = 1000;
    // 主题
    private String topic;
    // 版本号
    private int version = 0;
    // 顺序消息生产者Broker信息
    private SequentialBrokerState brokerState = new SequentialBrokerState();
    // 消费者状态列表
    private ConcurrentMap<String, AppConsumeState> appConsumeStates = new ConcurrentHashMap<String, AppConsumeState>(SEQUENTIAL_APP_SIZE);

    public SequentialTopicState() {
        //nothing to do
    }

    /**
     * 检查APP在group上当前是否是可读的
     *
     * @param group broker分组
     * @param app   应用
     * @return 是否可读
     */
    public boolean checkReadable(String group, String app) {
        if (group == null || app == null || group.isEmpty() || app.isEmpty()) {
            return false;
        }

        AppConsumeState consumeState = appConsumeStates.get(app);
        if (consumeState == null) {
            return false;
        }

        SequentialBrokerState.SequentialBroker readableBroker = consumeState.fetchReadableBroker();
        if (readableBroker != null && readableBroker.getGroup() != null && readableBroker.getGroup().equals(group)) {
            return true;
        }

        return false;
    }

    /**
     * 整理brokerState 将已经没有积压消息的broker从队尾移除掉
     *
     * @return
     */
    public boolean sinkSequentialBrokers() {
        if (brokerState.getSequentialBrokers().size() < 2) {
            return false;
        }

        SequentialBrokerState.SequentialBroker produceSequentialBroker = brokerState.fetchReadableBroker();
        for (AppConsumeState consumeState : appConsumeStates.values()) {
            SequentialBrokerState consumeBrokerState = consumeState.getBrokerState();
            // 如果当前group还在消费列表里，说明他上边还有积压
            if (consumeBrokerState.hasBacklogs(produceSequentialBroker.getGroup())) {
                //有应用还有积压
                return false;
            }
        }
        return brokerState.removeIdledGroup(produceSequentialBroker);
    }

    /**
     * 获取消费者状态
     *
     * @param app 应用
     * @return 消费者状态
     */
    public AppConsumeState fetchAndCreateConsumeState(String app) {
        if (app == null) {
            return null;
        }
        AppConsumeState consumeState = appConsumeStates.get(app);
        if (consumeState == null) {
            consumeState = new AppConsumeState(app);
            consumeState.setBrokerState(brokerState.clone());
            AppConsumeState old = appConsumeStates.putIfAbsent(app, consumeState);
            if (old != null) {
                consumeState = old;
            }
        }
        return consumeState;
    }

    /**
     * 获取消费状态
     *
     * @param app 应用
     * @return 消费状态
     */
    public AppConsumeState fetchConsumeState(String app) {
        if (app == null) {
            return null;
        }
        return appConsumeStates.get(app);
    }


    /**
     * 获取消费状态
     *
     * @param app 应用
     * @return 消费状态
     */
    public SequentialBrokerState.SequentialBroker fetchReadableBroker(String app) {
        if (app == null) {
            return null;
        }

        SequentialBrokerState.SequentialBroker readableBroker=null;
        AppConsumeState consumeState =  appConsumeStates.get(app);
        if (consumeState != null){
            SequentialBrokerState consumeBrokerState = consumeState.getBrokerState();
            if (consumeBrokerState != null){
                readableBroker = consumeBrokerState.fetchReadableBroker();
            }
        }
        return readableBroker;
    }

    /**
     * 重置消费者状态
     *
     * @param state 消费状态
     * @param app   应用
     */
    public void resetAppConsumeState(AppConsumeState state, String app) {
        if (state == null || app == null) {
            return;
        }
        appConsumeStates.put(app, state);
    }


    /**
     * 判断是否具有可写的Broker
     *
     * @return Y/N
     */
    public boolean hasWritableBroker() {
        return brokerState != null && brokerState.hasWritableBroker();
    }


    /**
     * 复制一下当前的状体
     *
     * @return 当前的主题状态
     */
    public SequentialTopicState clone() {
        SequentialTopicState topicState = new SequentialTopicState(this.topic);
        topicState.setBrokerState(this.brokerState.clone());
        topicState.setVersion(this.version);

        ConcurrentMap<String, AppConsumeState> consumeStates = new ConcurrentHashMap<String, AppConsumeState>
                (SEQUENTIAL_APP_SIZE);
        for (Map.Entry<String, AppConsumeState> entry : appConsumeStates.entrySet()) {
            AppConsumeState consumeState = entry.getValue() == null ? null : entry.getValue().clone();
            consumeStates.put(entry.getKey(), consumeState);
        }
        topicState.setAppConsumeStates(consumeStates);

        return topicState;
    }



    public boolean removeAllBrokers(){
        boolean changed = brokerState.removeAllBroker();

        for (AppConsumeState consumeState : appConsumeStates.values()){
            SequentialBrokerState state = consumeState.getBrokerState();
            if (state != null){
                if (state.removeAllBroker()){
                    changed = true;
                }
            }
        }
        return changed;
    }

    public boolean removeBrokers(List<SequentialBrokerState.SequentialBroker> brokers){
        boolean changed = brokerState.removeBroker(brokers);
        for (AppConsumeState consumeState : appConsumeStates.values()){
            SequentialBrokerState state = consumeState.getBrokerState();
            if (state != null){
                if (state.removeBroker(brokers)) {
                    Deque<SequentialBrokerState.SequentialBroker> seqBrokers = state.getSequentialBrokers();
                    if (seqBrokers == null || seqBrokers.isEmpty()) {
                        seqBrokers = new LinkedList<SequentialBrokerState.SequentialBroker>(brokerState.getSequentialBrokers());
                        state.setSequentialBrokers(seqBrokers);
                    }
                    changed = true;
                }
            }
        }
        return changed;
    }


    public SequentialTopicState(String topic) {
        if (topic == null) {
            throw new IllegalStateException("Topic can not be null");
        }
        this.topic = topic;
    }

    public ConcurrentMap<String, AppConsumeState> getAppConsumeStates() {
        return appConsumeStates;
    }

    public void setAppConsumeStates(ConcurrentMap<String, AppConsumeState> appConsumeStates) {
        if (appConsumeStates != null) {
            this.appConsumeStates = appConsumeStates;
        }
    }

    public SequentialBrokerState getBrokerState() {
        return brokerState;
    }

    public void setBrokerState(SequentialBrokerState brokerState) {
        if (brokerState != null)
            this.brokerState = brokerState;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "SequentialTopicState{" +
                "topic='" + topic + '\'' +
                ", version=" + version +
                ", brokerState=" + brokerState +
                ", appConsumeStates=" + appConsumeStates +
                '}';
    }
}
