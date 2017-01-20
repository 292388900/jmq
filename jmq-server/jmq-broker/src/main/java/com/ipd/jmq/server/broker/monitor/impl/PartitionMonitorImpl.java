package com.ipd.jmq.server.broker.monitor.impl;

import com.alibaba.fastjson.JSONObject;
import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.cluster.ClusterRole;
import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.monitor.*;
import com.ipd.jmq.replication.ReplicationMaster;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
import com.ipd.jmq.server.broker.monitor.api.PartitionMonitor;
import com.ipd.jmq.server.broker.offset.TopicOffset;
import com.ipd.jmq.server.broker.profile.PubSubStat;
//import com.ipd.jmq.server.broker.retry.RetryManager;
import com.ipd.jmq.server.store.Store;
import com.ipd.jmq.server.store.StoreConfig;
import com.ipd.jmq.server.store.journal.FileStat;
import com.ipd.jmq.toolkit.stat.TPStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by zhangkepeng on 16-11-30.
 */
public class PartitionMonitorImpl implements PartitionMonitor {

    private static final Logger logger = LoggerFactory.getLogger(PartitionMonitorImpl.class);

    // 集群管理
    protected ClusterManager clusterManager;
    // 派发服务
    protected DispatchService dispatchService;
    // 重试服务
//    protected RetryManager retryManager;
    // 当前Broker
    protected Broker broker;
    // 存储
    protected Store store;
    // 配置
    protected BrokerConfig brokerConfig;
    // 存储配置
    protected StoreConfig storeConfig;
    // 统计基础汇总信息
    protected BrokerStat brokerStat;
    // 统计性能
    protected BrokerPerfBuffer brokerPerfBuffer;
    // 异常统计
    protected BrokerExeBuffer brokerExeBuffer;
    // 复制主分片
    protected ReplicationMaster replicationMaster;
    // 分片启动时间
    protected long brokerStartTime;
    // 客户端统计信息
    protected PubSubStat pubSubStat;

    public PartitionMonitorImpl(Builder builder) {
        this.store = builder.store;
        this.brokerConfig = builder.brokerConfig;
        this.storeConfig = brokerConfig.getStoreConfig();
        this.clusterManager = builder.clusterManager;
        this.dispatchService = builder.dispatchService;
//        this.retryManager = builder.retryManager;
        this.replicationMaster = builder.replicationMaster;
        this.pubSubStat = builder.pubSubStat;
        this.broker = builder.broker;
        this.brokerStat = builder.brokerStat;
        this.brokerPerfBuffer = builder.brokerPerfBuffer;
        this.brokerExeBuffer = builder.brokerExeBuffer;
        this.brokerStartTime = builder.brokerStartTime;
    }

    @Override
    public BrokerStat getBrokerStat() {
        // 由于客户端移除后需要保留之前数据(可能用于恢复)但这些数据会导致报警，因此采用复制有用数据的方式
        BrokerStat myBrokerStat = new BrokerStat();

        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
        FileStat diskFileStat = store.getDiskFileStat();
        //计算需要实时获取的数据
        brokerStat.getDiskTotal().set(storeConfig.getMaxDiskSpace());
        brokerStat.getDiskUsed().set(diskFileStat.getAppendFileLength());
        brokerStat.getJvmHeapTotal().set(heapMemoryUsage.getMax());
        brokerStat.getJvmHeapUsed().set(heapMemoryUsage.getUsed());
        brokerStat.getMmapTotal().set(storeConfig.getJournalConfig().getCacheSize() + storeConfig.getQueueConfig()
                .getCacheSize());
        brokerStat.getMmapUsed().set(diskFileStat.getMappedMemory());
        brokerStat.setDiskWriteSpeed(calcDiskSpeed(diskFileStat.slice().getWriteStat().getTPStat()));
        if (replicationMaster != null) {
            brokerStat.setReplicationSpeed(replicationMaster.getRepStat().slice().getRepSpeed());
        }
        //获取配置信息
        brokerStat.setPort(clusterManager.getBroker().getPort());
        brokerStat.setAlias(clusterManager.getBroker().getAlias());
        brokerStat.setGroup(clusterManager.getBroker().getGroup());
        brokerStat.setDataCenter(clusterManager.getBroker().getDataCenter());
        brokerStat.setRole(clusterManager.getBroker().getRole());
        brokerStat.setPermission(clusterManager.getBroker().getPermission());
        brokerStat.setRetryType(clusterManager.getBroker().getRetryType());

        // 积压数量
        long totalPending = 0;
        long pending;
        long retryCount;
        TopicStat topicStat, myTopicStat;
        TopicOffset topicOffset;
        AppStat oldAppStat;
        ConcurrentMap<String, AppStat> myAppStats;
        TopicConfig topicConfig;
        // 判断存储是否启用成功
        if (store.isReady()) {
            brokerStat.setHasBadBlock(store.hasBadBlock());
            brokerStat.setMaxJournalOffset(store.getMaxOffset());
            brokerStat.setMinJournalOffset(store.getMinOffset());
            // 遍历消费队列
            ConcurrentMap<String, List<Integer>> queues = store.getQueues();
            for (String topic : queues.keySet()) {
                // 得到主题统计
                topicStat = brokerStat.getAndCreateTopicStat(topic);
                // 主题偏移量
                topicOffset = dispatchService.getOffset(topic);
                // 该主题还没有订阅
                if (topicOffset == null) {
                    continue;
                }
                // 复制一份数据
                myTopicStat = myBrokerStat.getAndCreateTopicStat(topic);
                myAppStats = myTopicStat.getAppStats();
                myTopicStat.setTopic(topicStat.getTopic());
                myTopicStat.getProducer().set(topicStat.getProducer().get());
                myTopicStat.getConsumer().set(topicStat.getConsumer().get());
                myTopicStat.getEnQueue().set(topicStat.getEnQueue().get());
                myTopicStat.getEnQueueSize().set(topicStat.getEnQueueSize().get());
                myTopicStat.getDeQueue().set(topicStat.getDeQueue().get());
                myTopicStat.getDeQueueSize().set(topicStat.getDeQueueSize().get());

                // 主题配置
                topicConfig = clusterManager.getTopicConfig(topic);

                if (topicConfig != null) {
                    // 遍历生产者
                    for (String producer : topicConfig.getProducers().keySet()) {
                        // 得到应用统计
                        AppStat appStat = topicStat.getAppStat(producer);
                        AppStat myAppStat = new AppStat(producer);
                        myAppStat.setProducerRole(true);
                        if (appStat != null) {
                            myAppStat.setApp(appStat.getApp());
                            myAppStat.setClients(appStat.getClients());
                            myAppStat.getConnection().set(appStat.getConnection().get());
                            myAppStat.getEnQueue().set(appStat.getEnQueue().get());
                            myAppStat.getDeQueue().set(appStat.getDeQueue().get());
                            myAppStat.getDeQueueSize().set(appStat.getDeQueueSize().get());
                            myAppStat.getEnQueueSize().set(appStat.getEnQueueSize().get());
                            myAppStat.getProducer().set(appStat.getProducer().get());
                            myAppStat.getConsumer().set(appStat.getConsumer().get());
                        }
                        if (myAppStat.getProducer().get() > 0 || topicConfig == null || broker == null || assigmentToBroker(topicConfig, broker)) {
                            myAppStats.putIfAbsent(producer, myAppStat);
                        }
                    }
                }
                // 遍历消费位置
                for (String consumer : topicOffset.getConsumers().keySet()) {

                    AppStat myAppStat = new AppStat(consumer);//myTopicStat.getAndCreateAppStat(consumer);
                    myAppStat.setConsumerRole(true);
                    AppStat appStat = topicStat.getAppStat(consumer);
                    if (appStat != null) {
                        myAppStat.setApp(appStat.getApp());
                        myAppStat.setClients(appStat.getClients());
                        myAppStat.getConnection().set(appStat.getConnection().get());
                        myAppStat.getEnQueue().set(appStat.getEnQueue().get());
                        myAppStat.getDeQueue().set(appStat.getDeQueue().get());
                        myAppStat.getDeQueueSize().set(appStat.getDeQueueSize().get());
                        myAppStat.getEnQueueSize().set(appStat.getEnQueueSize().get());
                        myAppStat.getProducer().set(appStat.getProducer().get());
                        myAppStat.getConsumer().set(appStat.getConsumer().get());
                    }

                    // 积压数据
                    try {
                        pending = dispatchService.getPending(topicStat.getTopic(), myAppStat.getApp());
                    } catch (Exception e) {
                        logger.error("get pending error:" + e.getMessage(), e);
                        pending = 0;
                    }
                    myAppStat.getPending().set(pending);
                    // 重试数据
                    retryCount = getRetry(topicStat.getTopic(), myAppStat.getApp());
                    myAppStat.getRetry().set(retryCount);
                    if (pending > 0 || topicConfig == null || broker == null || assigmentToBroker(topicConfig, broker) || myAppStat.getConsumer().get() > 0) {
                        oldAppStat = myAppStats.putIfAbsent(consumer, myAppStat);
                        if (oldAppStat != null) {
                            oldAppStat.setConsumerRole(true);
                            oldAppStat.getPending().set(pending);
                            oldAppStat.getRetry().set(retryCount);
                        }
                    }

                    totalPending += pending;
                }
            }
        }
        // 设置总的积压数量
        brokerStat.getPending().set(totalPending);

        // 复制数据
        myBrokerStat.setName(brokerStat.getName());
        myBrokerStat.getConnection().set(brokerStat.getConnection().get());
        myBrokerStat.getEnQueue().set(brokerStat.getEnQueue().get());
        myBrokerStat.getEnQueueSize().set(brokerStat.getEnQueueSize().get());
        myBrokerStat.getDeQueue().set(brokerStat.getDeQueue().get());
        myBrokerStat.getDeQueueSize().set(brokerStat.getDeQueueSize().get());
        myBrokerStat.getPending().set(brokerStat.getPending().get());
        myBrokerStat.getJvmHeapTotal().set(brokerStat.getJvmHeapTotal().get());
        myBrokerStat.getJvmHeapUsed().set(brokerStat.getJvmHeapUsed().get());
        myBrokerStat.getDiskTotal().set(brokerStat.getDiskTotal().get());
        myBrokerStat.getDiskUsed().set(brokerStat.getDiskUsed().get());
        myBrokerStat.getMmapTotal().set(brokerStat.getMmapTotal().get());
        myBrokerStat.getMmapUsed().set(brokerStat.getMmapUsed().get());
        myBrokerStat.setDiskWriteSpeed(brokerStat.getDiskWriteSpeed());
        myBrokerStat.setReplicationSpeed(brokerStat.getReplicationSpeed());
        myBrokerStat.setMaxJournalOffset(brokerStat.getMaxJournalOffset());
        myBrokerStat.setMinJournalOffset(brokerStat.getMinJournalOffset());
        myBrokerStat.setHasBadBlock(brokerStat.isHasBadBlock());
        myBrokerStat.setPort(brokerStat.getPort());
        myBrokerStat.setAlias(brokerStat.getAlias());
        myBrokerStat.setGroup(brokerStat.getGroup());
        myBrokerStat.setDataCenter(brokerStat.getDataCenter());
        myBrokerStat.setRole(brokerStat.getRole());
        myBrokerStat.setPermission(brokerStat.getPermission());
        myBrokerStat.setRetryType(brokerStat.getRetryType());

        return myBrokerStat;
    }

    // 是否分配给该分片
    private boolean assigmentToBroker(TopicConfig topicConfig, Broker broker) {
        if (topicConfig != null && broker != null
                && !topicConfig.getGroups().isEmpty() && broker.getGroup() != null
                && !broker.getGroup().isEmpty()
                && topicConfig.getGroups().contains(broker.getGroup())) {
            return true;
        } else {
            return false;
        }
    }

    private double calcDiskSpeed(TPStat stat) {
        long size = stat.getSize();
        long time = stat.getTime();
        double diskSpeed = 0.0;
        if (time > 0) {
            BigDecimal bg = new BigDecimal(size * 1000000000.0 / time);
            diskSpeed = bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
        }
        return diskSpeed;
    }

    // 重试数据
    private int getRetry(String topic, String app) {
        try {
            return -1;///retryManager.getRetry(topic, app);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return 0;
        }
    }

    /**
     * 获取分片性能统计
     * @return
     */
    @Override
    public BrokerPerf getPerformance() {
        return brokerPerfBuffer.slice();
    }

    /**
     * 获取分片异常统计
     * @return
     */
    @Override
    public BrokerExe getBrokerExe() {
        return brokerExeBuffer.slice();
    }

    /**
     * 获取复制状态
     * @return
     */
    @Override
    public List<ReplicationState> getReplicationStates() {
        List<ReplicationState> replicationStates = new ArrayList<ReplicationState>();
        ReplicationState replicationState = new ReplicationState();
        if (replicationMaster != null && replicationMaster.getInsyncReplicas() != null) {
            replicationState.setOnlineReplicas(replicationMaster.getInsyncReplicas().size());
        }
        replicationState.setStoreRestartTimes(store.getRestartTimes());
        replicationState.setBroker(broker.getName());
        replicationStates.add(replicationState);
        return replicationStates;
    }

    @Override
    public boolean hasReplicas() {
        if (replicationMaster != null) {
            return replicationMaster.getAllReplicas().size() > 0;
        }
        return false;
    }

    /**
     * 获取当前分片角色
     * @return
     */
    @Override
    public ClusterRole getBrokerRole() {
        return broker.getRole();
    }

    /**
     * 获取服务端broker版本信息
     * @return
     */
    @Override
    public String getBrokerVersion() {
        String version = this.getClass().getPackage().getImplementationVersion();
        return version == null || version.isEmpty() ? "Unknown" : version;
    }

    @Override
    public String restartAndStartTime() {
        return this.brokerStartTime + ";" + getBrokerVersion() + "," + store.getRestartTimes() + "/" + store.getRestartSuccess();
    }

    // 获取存储配置
    @Override
    public String getStoreConfig() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("cleanInterval", storeConfig.getCleanInterval());
        jsonObject.put("queueFlushFileInterval", storeConfig.getQueueFlushFileInterval());
        jsonObject.put("recoverQueueFiles", storeConfig.getRecoverQueueFiles());
        jsonObject.put("bufferPoolSize", storeConfig.getBufferPoolSize());
        jsonObject.put("JournalFlushPolicy.FlushInterval", storeConfig.getJournalFlushPolicy().getFlushInterval());
        jsonObject.put("JournalFlushPolicy.isSync", storeConfig.getJournalFlushPolicy().isSync());
        jsonObject.put("QueueFlushPolicy.isSync", storeConfig.getQueueFlushPolicy().isSync());
        jsonObject.put("QueueFlushPolicy.FlushInterval", storeConfig.getQueueFlushPolicy().getFlushInterval());
        return jsonObject.toJSONString();
    }

    public static class Builder {
        // 存储
        protected Store store;
        // 配置
        protected BrokerConfig brokerConfig;
        // 集群管理
        protected ClusterManager clusterManager;
        // 派发服务
        protected DispatchService dispatchService;
        // 重试服务
//        protected RetryManager retryManager;
        // 复制主分片
        protected ReplicationMaster replicationMaster;
        // 统计基础汇总信息
        protected BrokerStat brokerStat;
        // 统计性能
        protected BrokerPerfBuffer brokerPerfBuffer;
        // 异常统计
        protected BrokerExeBuffer brokerExeBuffer;
        // 客户端统计信息
        protected PubSubStat pubSubStat;
        // 当前Broker
        protected Broker broker;
        //启动时间
        protected long brokerStartTime;

        public Builder(Store store,BrokerConfig brokerConfig) {
            this.store = store;
            this.brokerConfig = brokerConfig;
        }

        public Builder clusterManager(ClusterManager clusterManager) {
            this.clusterManager = clusterManager;
            return this;
        }

        public Builder dispatchService(DispatchService dispatchService) {
            this.dispatchService = dispatchService;
            return this;
        }

//        public Builder retryManager(RetryManager retryManager) {
//            this.retryManager = retryManager;
//            return this;
//        }

        public Builder replicationMaster(ReplicationMaster replicationMaster) {
            this.replicationMaster = replicationMaster;
            return this;
        }

        public Builder brokerStat(BrokerStat brokerStat) {
            this.brokerStat = brokerStat;
            return this;
        }

        public Builder brokerPerfBuffer(BrokerPerfBuffer brokerPerfBuffer) {
            this.brokerPerfBuffer = brokerPerfBuffer;
            return this;
        }

        public Builder brokerExeBuffer(BrokerExeBuffer brokerExeBuffer) {
            this.brokerExeBuffer = brokerExeBuffer;
            return this;
        }

        public Builder pubSubStat(PubSubStat pubSubStat) {
            this.pubSubStat = pubSubStat;
            return this;
        }

        public Builder broker(Broker broker) {
            this.broker = broker;
            return this;
        }

        public Builder brokerStartTime(long brokerStartTime){
            this.brokerStartTime = brokerStartTime;
            return this;
        }

        public PartitionMonitor build() {
            return new PartitionMonitorImpl(this);
        }
    }
}
