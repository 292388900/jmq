package com.ipd.jmq.server.broker.monitor;

import com.ipd.jmq.common.cluster.ClusterRole;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.lb.TopicSession;
import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.model.OffsetInfo;
import com.ipd.jmq.common.monitor.*;
import com.ipd.jmq.server.broker.monitor.api.*;
import com.ipd.jmq.toolkit.service.Service;

import java.util.List;

/**
 * Created by zhangkepeng on 16-11-30.
 */
public class BrokerMonitorTool extends Service{

    // 重启时间
    protected long brokerStartTime;
    // 分片监控
    protected PartitionMonitor partitionMonitor;
    // 主题或生产者与消费者监控
    protected TopicMonitor topicMonitor;
    // 生产监控
    protected ProducerMonitor producerMonitor;
    // 消费监控
    protected ConsumerMonitor consumerMonitor;
    // 客户端监控
    protected SessionMonitor sessionMonitor;

    // 获取分片统计信息
    public BrokerStat getBrokerStat() {
        return partitionMonitor.getBrokerStat();
    }

    public BrokerPerf getPerformance() {
        return partitionMonitor.getPerformance();
    }

    public BrokerExe getBrokerExe() {
        return partitionMonitor.getBrokerExe();
    }

    public List<ReplicationState> getReplicationStates() {
        return partitionMonitor.getReplicationStates();
    }

    public boolean hasReplicas() {
        return partitionMonitor.hasReplicas();
    }

    public ClusterRole getBrokerRole() {
        return partitionMonitor.getBrokerRole();
    }

    public String getBrokerVersion() {
        return partitionMonitor.getBrokerVersion();
    }

    public String restartAndStartTime(){
        return partitionMonitor.restartAndStartTime();
    }

    public String getStoreConfig(){
        return partitionMonitor.getStoreConfig();
    }

    public long getBrokerStartTime() {
        return brokerStartTime;
    }

    public void setBrokerStartTime(long brokerStartTime) {
        this.brokerStartTime = brokerStartTime;
    }

    // 增加消费者
    public void subscribe(String topic, String app) {
        consumerMonitor.subscribe(topic, app);
    }

    // 删除消费者
    public void unsubscribe(String topic, String app) {
        consumerMonitor.unsubscribe(topic, app);
    }

    public List<BrokerMessage> viewMessage(String topic, String app, int count) {
        return consumerMonitor.viewMessage(topic, app, count);
    }

    // 获取消费者指定队列的消费位置信息
    public OffsetInfo getOffsetInfo(String topic, String app, int queueId) {
        return consumerMonitor.getOffsetInfo(topic, (short)queueId, app);
    }

    public void resetAckOffset(String topic, short queueId, String app, long offsetOrTime, boolean isOffset) throws
            JMQException {
        consumerMonitor.resetAckOffset(topic, queueId, app, offsetOrTime, isOffset);
    }

    // 获取没有ack的blocks
    public String getNoAckBlocks() {
        return consumerMonitor.getNoAckBlocks();
    }

    // 获取location
    public List<String> getLocations(final String topic, final String app) {
        return consumerMonitor.getLocations(topic, app);
    }

    // 强制释放location
    public boolean cleanExpire(final String topic, final String app, final short queueId) {
        return consumerMonitor.cleanExpire(topic, app, queueId);
    }

    // 获取客户端信息
    public List<Client> getConnections() {
        return sessionMonitor.getConnections();
    }

    /**
     * 获取指定应用的客户端信息
     * @param topic
     * @param app
     * @return
     */
    public List<Client> getConnections(final String topic, final String app) {
        return sessionMonitor.getConnections(topic, app);
    }

    /**
     * 关闭生产者
     * @param topic
     * @param app
     */
    public void closeProducer(final String topic, final String app) {
        sessionMonitor.closeProducer(topic, app);
    }

    /**
     * 关闭消费者
     * @param topic
     * @param app
     */
    public void closeConsumer(final String topic, final String app) {
        sessionMonitor.closeConsumer(topic, app);
    }

    /**
     * 获取会话信息
     * @return
     */
    public List<TopicSession> getTopicSessionsOnBroker() {
        return sessionMonitor.getTopicSessionsOnBroker();
    }
}
