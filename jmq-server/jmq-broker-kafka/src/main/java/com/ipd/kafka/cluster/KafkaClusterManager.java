package com.ipd.kafka.cluster;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ipd.jmq.common.cluster.*;
import com.ipd.jmq.common.network.kafka.model.*;
import com.ipd.jmq.common.network.kafka.utils.ZKUtils;
import com.ipd.jmq.registry.listener.*;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.cluster.*;
import com.ipd.jmq.server.broker.election.RoleDecider;
import com.ipd.jmq.server.broker.election.RoleEvent;
import com.ipd.jmq.toolkit.concurrent.EventBus;
import com.ipd.jmq.toolkit.concurrent.NamedThreadFactory;
import com.ipd.jmq.toolkit.io.Zip;
import com.ipd.kafka.mapping.KafkaMapService;
import com.ipd.jmq.server.broker.utils.BrokerUtils;
import com.ipd.jmq.common.network.ServerConfig;
import com.ipd.jmq.registry.PathData;
import com.ipd.jmq.registry.Registry;
import com.ipd.jmq.registry.RegistryException;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.io.Compressors;
import com.ipd.jmq.toolkit.io.Zlib;
import com.ipd.jmq.toolkit.lang.Charsets;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Created by zhangkepeng on 16-8-9.
 * <p/>
 * １、获取元数据并缓存
 * ２、更新zookeeper相关节点信息
 */
public class KafkaClusterManager extends Service {
    private final static Logger logger = LoggerFactory.getLogger(KafkaClusterManager.class);

    public static final String ALIVE_BROKERS_FILE_NAME = "alive_brokers";
    public static final String TOPIC_PARTITIONS_FILE_NAME = "topic_partitions";

    // kafka集群事件管理器
    private EventBus<KafkaClusterEvent> eventManager = new EventBus<KafkaClusterEvent>("KafkaClusterManager");
    // 角色监听
    private RoleDecider roleDecider;
    // 集群管理
    private ClusterManager clusterManager;
    // broker配置
    private BrokerConfig brokerConfig;
    // Kafka健康检查
    private KafkaHealthcheck kafkaHealthcheck;
    // 注册中心
    private Registry registry;
    // 服务配置
    private ServerConfig serverConfig;
    // 更新前本地分组主题
    private Map<String, Set<Integer>> localLastTopics = new HashMap<String, Set<Integer>>();
    // 集群监听器
    private ClusterListener clusterListener = new ClusterListener();
    // ZK角色变化分布式通知器
    private RoleChangeListener roleChangeListener = new RoleChangeListener();
    // 主题partition分布式通知器
    private TopicPartitionChangeListener topicPartitionChangeListener = new TopicPartitionChangeListener();
    // 本地角色编号监听器
    private RoleListener roleListener = new RoleListener();
    // leader变化监听器
    private KafkaLeaderListener kafkaLeaderListener = new KafkaLeaderListener();
    // topic partition 监听器
    private TopicPartitionListener partitionListener = new TopicPartitionListener();
    // 读写锁
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    // 写锁
    private final Lock mutex;
    // 缓存一致性校验
    protected ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory
                    ("JMQ_SERVER_UPDATE_KAFKA_CACHE_SCHEDULE"));

    // 分片
    public static Broker localBroker;
    // 分组
    public static BrokerGroup brokerGroup;
    // kafka分片
    public static KafkaBroker kafkaBroker;
    // kafka replicas集合
    public static Set<Integer> ids = new HashSet<Integer>(3);
    // kafka元数据更新器
    public static MetadataUpdater metadataUpdater;
    // leader_epoch
    public static int leaderEpoch = 0;
    // kafka版本
    public static int version = 1;
    // 初始赋值
    public static int initValue = -1;

    // 分片信息文件
    private File aliveBrokersFile;
    // 主题分片配置信息文件
    private File topicPartitionsFile;
    // 是否本地加载元信息
    private boolean loadAliveBrokers = true;
    private boolean loadTopicPartitions = true;

    public KafkaClusterManager(RoleDecider roleDecider, ClusterManager clusterManager, BrokerConfig brokerConfig) {
        this.mutex = this.rwLock.writeLock();
        this.roleDecider = roleDecider;
        this.brokerConfig = brokerConfig;
        this.clusterManager = clusterManager;
        this.metadataUpdater = new MetadataUpdater(eventManager, brokerConfig, this);
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        Preconditions.checkArgument(roleDecider != null, "RoleDecider can not be null");
        Preconditions.checkArgument(clusterManager != null, "ClusterManager can not be null");
        Preconditions.checkArgument(brokerConfig != null, "BrokerConfig can not be null");
        this.registry = brokerConfig.getRegistry();
        this.serverConfig = brokerConfig.getServerConfig();
        Preconditions.checkArgument(registry != null, "Registry can not be null");
        Preconditions.checkArgument(serverConfig != null, "ServerConfig can not be null");
    }

    @Override
    public void doStart() throws Exception {
        super.doStart();
        // 构造本地KafkaBroker
        kafkaBroker = new KafkaBroker(initValue, serverConfig.getIp(), initValue);
        // 更新分片元信息
        updateKafkaBroker();

        if (kafkaBroker.getId() != initValue) {
            // 注册leader监听
            registry.addListener(ZKUtils.getBrokerLeaderPath(), kafkaLeaderListener);
        }
        // 事件管理器
        eventManager.start();

        /**
         * updateKafkaTopicPartition() 会触发KafkaClusterEvent事件
         * 因此需要先start(),其方法内添加了eventManager的listener
         */
        metadataUpdater.start();

        // 更新队列元信息
        updateKafkaTopicPartition();
        if (brokerConfig.isUseLocalConfig()) {
            if (loadAliveBrokers) {
                loadAliveBrokers();
            }
            if (loadTopicPartitions) {
                loadTopicPartitions();
            }
        }
        // 监听角色变化
        roleDecider.addListener(roleListener);
        // 角色变化通过ZK节点通知其它分片
        registry.addListener(ZKUtils.getRoleChangeParentPath(), roleChangeListener);
        // 主题partition变化监听器
        registry.addListener(ZKUtils.brokerTopicsPath, topicPartitionChangeListener);
        // 监听主题队列变化ZK节点
        registry.addListener(ZKUtils.getTopicPartitionPath(), partitionListener);
        // 监听集群元数据变化
        clusterManager.addListener(clusterListener);

        // 创建存活节点
        if (kafkaBroker.getId() != initValue) {
            // 注册本地Broker
            kafkaHealthcheck = new KafkaHealthcheck(kafkaBroker.getId(), kafkaBroker.getHost(), kafkaBroker.getPort(), registry);
            kafkaHealthcheck.startup();
        }
        // 元数据缓存更新器
        metadataUpdater.updateMetadataCache();
        // 定时更新缓存
        fixRateUpdateKafkaCache();
    }

    @Override
    public void doStop() {
        super.doStop();
        roleDecider.removeListener(roleListener);
        clusterManager.removeListener(clusterListener);
        registry.removeListener(ZKUtils.getBrokerLeaderPath(), kafkaLeaderListener);
        registry.removeListener(ZKUtils.getRoleChangeParentPath(), roleChangeListener);
        registry.removeListener(ZKUtils.brokerTopicsPath, topicPartitionChangeListener);
        registry.removeListener(ZKUtils.getTopicPartitionPath(), partitionListener);
        if (kafkaHealthcheck != null) {
            kafkaHealthcheck.shutdown();
        }
        eventManager.stop();
        metadataUpdater.stop();
    }

    private void fixRateUpdateKafkaCache() {
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    // 更新元数据信息
                    updateKafkaBroker();
                    updateKafkaTopicPartition();
                    metadataUpdater.updateMetadataCache();
                } catch (Exception e) {
                    logger.error("ScheduledTask updateCache exception", e);
                }
            }
        }, 1, brokerConfig.getUpdataKafkaCachePeriod(), TimeUnit.MINUTES);
    }

    private void loadAliveBrokers() {
        String content = null;
        try {
            aliveBrokersFile = new File(clusterManager.getAndCreateConfigDir(), TOPIC_PARTITIONS_FILE_NAME);
            content = (String) BrokerUtils.readConfigFile(aliveBrokersFile, String.class, "");
            List<Broker> aliveBrokers = JSON.parseArray(content, Broker.class);
            if (aliveBrokers != null && !aliveBrokers.isEmpty()) {
                updateKafkaBroker(aliveBrokers);
            }
        } catch (Exception e) {
            logger.error(String.format("Init local brokers error! content:%s", content), e);
        }
    }

    private void loadTopicPartitions() {
        String content = null;
        try {
            topicPartitionsFile = new File(clusterManager.getAndCreateConfigDir(), TOPIC_PARTITIONS_FILE_NAME);
            content = (String) BrokerUtils.readConfigFile(topicPartitionsFile, String.class, "");
            List<TopicQueues> topicQueueses = JSON.parseArray(content, TopicQueues.class);
            if (topicQueueses != null && !topicQueueses.isEmpty()) {
                updateKafkaTopicPartition(topicQueueses);
            }
        } catch (Exception e) {
            logger.error(String.format("Init local topic partitions error! content:%s", content), e);
        }
    }

    private void saveAliveBrokers(String aliveBrokersStr) {
        try {
            if (aliveBrokersStr == null || aliveBrokersStr.isEmpty()) {
                return;
            }

            if (aliveBrokersFile == null) {
                aliveBrokersFile = new File(clusterManager.getAndCreateConfigDir(), ALIVE_BROKERS_FILE_NAME);
            }

            BrokerUtils.writeConfigFile(aliveBrokersFile, aliveBrokersStr);
        } catch (Throwable th) {
            logger.error("Save alive brokers error!", th);
        }
    }

    private void saveTopicPartitions(String content) {
        try {
            if (content == null || content.isEmpty()) {
                return;
            }
            if (topicPartitionsFile == null) {
                topicPartitionsFile = new File(clusterManager.getAndCreateConfigDir(), TOPIC_PARTITIONS_FILE_NAME);
            }
            BrokerUtils.writeConfigFile(topicPartitionsFile, content);
        } catch (Throwable th) {
            logger.error("Save topic partitions error!", th);
        }
    }

    /**
     * 当前节点如果变成主节点，由主节点负责向ZK角色变化节点更新数据
     *
     * @param roleEvent
     */
    public void updateRoleChangePath(RoleEvent roleEvent) {

        // 更新本地replicas集合
        int loaclBrkerId = kafkaBroker.getId();
        int masterBrokerId = roleEvent.getMaster().getId();
        // leader变化，更新epoch
        leaderEpoch++;
        if (loaclBrkerId == masterBrokerId) {
            List<Broker> brokers = roleEvent.getSlaves();
            synchronized (ids) {
                ids.clear();
                ids.add(masterBrokerId);
                for (Broker broker : brokers) {
                    ids.add(broker.getId());
                }
            }
            // 更新角色变化节点
            ZKUtils.updatePersistentPath(registry, ZKUtils.getRoleChangePath(), String.valueOf(kafkaBroker.getId()));
        }
    }

    /**
     * ClusterManager缓存变化、主从角色变化
     */
    public void updateKafkaBroker() throws Exception {
        List<Broker> brokers = new ArrayList<Broker>();
        Map<String, Broker> brokerMap = clusterManager.getBrokers();
        Set<String> brokerNames = brokerMap.keySet();
        for (String brokerName : brokerNames) {
            brokers.add(brokerMap.get(brokerName));
        }
        updateKafkaBroker(brokers);
        loadAliveBrokers = false;
    }

    /**
     * 主题配置映射
     */
    public Map<String, TopicConfig> getTopics() {
        return clusterManager.getTopics();
    }

    /**
     * 本地分组下主题对应的parition集合
     * @return
     */
    public Map<String, Set<Integer>> getLocalLastTopics() {
        return localLastTopics;
    }

    /**
     * 根据主题获取一个生产者应用代码
     * @param topic
     * @return
     */
    public List<String> getProducerApp(String topic) {
        Map<String, TopicConfig> topicConfigMap = clusterManager.getTopics();
        if (topicConfigMap != null) {
            TopicConfig topicConfig = topicConfigMap.get(topic);
            if (topicConfig != null) {
                Map<String, TopicConfig.ProducerPolicy> producerPolicyMap = topicConfig.getProducers();
                if (producerPolicyMap != null) {
                    Set<String> producerSet = producerPolicyMap.keySet();
                    List<String> producers = new ArrayList<String>(producerSet);
                    return producers;
                }
            }
        }
        return null;
    }

    /**
     * 根据主题获取消费者应用代码
     * @param topic
     * @return
     */
    public List<String> getConsumerApp(String topic) {
        Map<String, TopicConfig> topicConfigMap = clusterManager.getTopics();
        if (topicConfigMap != null) {
            TopicConfig topicConfig = topicConfigMap.get(topic);
            if (topicConfig != null) {
                Map<String, TopicConfig.ConsumerPolicy> consumerPolicyMap = topicConfig.getConsumers();
                if (consumerPolicyMap != null) {
                    Set<String> consumerSet = consumerPolicyMap.keySet();
                    List<String> consumers = new ArrayList<String>(consumerSet);
                    return consumers;
                }
            }
        }
        return null;
    }

    /**
     * 更新Broker
     *
     * @param brokers Broker信息
     */
    protected void updateKafkaBroker(List<Broker> brokers) throws Exception{
        if (brokers == null) {
            return;
        }
        Map<Integer, KafkaBroker> aliveBrokers = map2KafkaBrokers(brokers);
        // 更新缓存中broker信息
        metadataUpdater.metadataCache.setAliveBrokers(aliveBrokers);
        // 存储本地文件一份
        if (brokerConfig.isUseLocalConfig()) {
            String jsonString = JSONObject.toJSONString(brokers);
            saveAliveBrokers(jsonString);
        }
    }

    private Map<Integer, KafkaBroker>  map2KafkaBrokers(List<Broker> brokers) throws Exception {
        Map<Integer, KafkaBroker> aliveBrokers = new HashMap<Integer, KafkaBroker>();
        Map<String, BrokerGroup> groupMap = new HashMap<String, BrokerGroup>(brokers.size());
        String groupName = null;
        boolean isMatch = false;
        for (Broker broker : brokers) {
            KafkaBroker newKafkaBroker = new KafkaBroker(broker.getId(), broker.getIp(), broker.getPort());
            aliveBrokers.put(broker.getId(), newKafkaBroker);
            // 获取分组
            BrokerGroup bGroup = groupMap.get(broker.getGroup());
            if (bGroup == null) {
                // 分组不存在，则创建
                bGroup = new BrokerGroup();
                bGroup.setGroup(broker.getGroup());
                groupMap.put(broker.getGroup(), bGroup);
            }
            // 添加分组包含的分片
            bGroup.addBroker(broker);
            // 判断是否是当前分组，当前分组为主，负责将信息注册到注册中心
            if (broker.getIp().equals(kafkaBroker.getHost())) {
                // 本地Kafka Broker
                kafkaBroker.setId(broker.getId());
                kafkaBroker.setPort(broker.getPort());
                groupName = broker.getGroup();
                isMatch = true;
                localBroker = broker;
            }
        }
        if (!isMatch) {
            logger.error(String.format("kafka broker config maybe error,can not find local kafka broker %s", this.kafkaBroker));
            /*throw new Exception(String.format("kafka broker config maybe error,can not find local %s", this.kafkaBroker));*/
        } else {
            // 赋值本地分组
            brokerGroup = groupMap.get(groupName);
            if (brokerGroup != null) {
                synchronized (ids) {
                    for (Broker broker : brokerGroup.getBrokers()) {
                        ids.add(broker.getId());
                    }
                }
            }
            logger.info(String.format("kafka broker config right, find local kafka broker %s", this.kafkaBroker));
        }
        return aliveBrokers;
    }

    public void updateKafkaTopicPartition() {
        try {
            PathData pathData = registry.getData(ZKUtils.getTopicPartitionPath());
            if (pathData == null) {
                return;
            }

            updateKafkaTopicPartition(pathData.getData());
            loadTopicPartitions = false;
        } catch (Exception e) {
            logger.warn("update kafka error!", e);
        }
    }

    public void updateKafkaTopicPartition(byte[] data) {
        if (data == null || data.length < 1) {
            return;
        }

        try {
            if (brokerConfig.isCompressed()) {
                try {
                    data = Compressors.decompress(data, Zip.INSTANCE);
                } catch (IOException e) {
                    data = Compressors.decompress(data, Zlib.INSTANCE);
                }
            }

            String content = new String(data, Charsets.UTF_8);
            List<TopicQueues> queues = JSON.parseArray(content, TopicQueues.class);
            updateKafkaTopicPartition(queues);
            if (brokerConfig.isUseLocalConfig()) {
                saveTopicPartitions(content);
            }
        } catch (Exception e){
            logger.warn("update kafka error!", e);
        }

    }

    /**
     * 更新主题集群信息,将本地分组下主题对应的partition更新到缓存中，并写入ZK
     *
     * @param topicQueueses 主题集群信息
     */
    protected void updateKafkaTopicPartition(List<TopicQueues> topicQueueses) {
        if (topicQueueses == null) {
            return;
        }

        // 含有该分组主题与分片
        Map<String, Set<Integer>> topicPartitions = new HashMap<String, Set<Integer>>();
        // 本地分片下所有分片信息
        Set<Integer> localPartitions = new TreeSet<Integer>();
        // 本地分组下所有主题对应的队列编号（Partition）
        for (TopicQueues topicQueues : topicQueueses) {
            // 分组对应的队列号，及partition
            Map<String, List<Short>> groupQues = topicQueues.getGroups();
            if (groupQues != null) {
                Set<String> groups = groupQues.keySet();
                if (brokerGroup != null && groups != null) {
                    // 如果包含本地的分组
                    if (groups.contains(brokerGroup.getGroup())) {
                        for (Short queue : groupQues.get(brokerGroup.getGroup())) {
                            localPartitions.add((int) queue);
                        }
                        topicPartitions.put(topicQueues.getTopic(), localPartitions);
                    }
                }
            }
        }
        // 更新分片与队列映射关系,目前不考虑缩容与迁移
        KafkaMapService.updatePartition2Queue(localPartitions);
        // 同步更新ZK节点
        updateTopicPartitionNode(topicPartitions);
    }

    /**
     * 更新缓存
     */
    private void updateTopicPartitionNode(Map<String, Set<Integer>> topicPartitions) {
        if (isMaster()) {
            // 删除失效缓存数据
            Set<String> tmpTopics = new HashSet<String>();
            Set<String> topics = topicPartitions.keySet();
            Set<String> lastTopics;
            if (localLastTopics.isEmpty()) {
                List<String> zkTopics = ZKUtils.getChildren(registry, ZKUtils.brokerTopicsPath);
                lastTopics = new HashSet<String>(zkTopics);
            } else {
                lastTopics = localLastTopics.keySet();
            }
            if (!lastTopics.isEmpty()) {
                tmpTopics.addAll(lastTopics);
                tmpTopics.removeAll(topics);
                for (String deleteTopic : tmpTopics) {
                    logger.warn(String.format("%s isn't belong to %s", deleteTopic, brokerGroup.getGroup()));
                    Set<Integer> deletePartitions = localLastTopics.get(deleteTopic);
                    if (deletePartitions != null) {
                        for (int deletePartition : deletePartitions) {
                            // 是否删除ZK对应节点由controller决定
                            addPartitionUpdateEvent(deleteTopic, deletePartition, KafkaClusterEvent.EventType.PARTITION_REMOVED, kafkaBroker.getId());
                        }
                    }
                }
            }

            // 更新缓存
            for (String topic : topics) {
                // 主题partition是否发生变化
                boolean isLastTopicsEmpty;
                boolean isChange = false;
                Set<Integer> partitions = topicPartitions.get(topic);
                // 获取之前的partition对应数量,发送命令给controller删除不存在的partition节点
                Set<Integer> lastPartitions = new HashSet<Integer>();
                if (localLastTopics.isEmpty()) {
                    isLastTopicsEmpty = true;
                    List<String> zkPartitions = ZKUtils.getChildren(registry, ZKUtils.getTopicPartitionsPath(topic));
                    if (zkPartitions != null) {
                        for (String zkPartition : zkPartitions) {
                            int partitionNode = Integer.valueOf(zkPartition);
                            lastPartitions.add(partitionNode);
                        }
                    }
                } else {
                    isLastTopicsEmpty = false;
                    lastPartitions = localLastTopics.get(topic);
                }
                Set<Integer> tmpPartitions = new HashSet<Integer>();
                if (lastPartitions != null && !lastPartitions.isEmpty()) {
                    tmpPartitions.addAll(lastPartitions);
                    tmpPartitions.removeAll(partitions);
                    for (int deletePartition : tmpPartitions) {
                        // 通知controller更新节点，是否删除ZK对应节点由controller决定
                        addPartitionUpdateEvent(topic, deletePartition, KafkaClusterEvent.EventType.PARTITION_REMOVED, kafkaBroker.getId());
                        isChange = true;
                    }
                    if (lastPartitions.size() < partitions.size()) {
                        isChange = true;
                    }
                }

                if (isLastTopicsEmpty || isChange) {
                    // 更新最新的partition节点数据到ZK，发送命令到controller更新主题节点数据
                    synchronized (ids) {
                        for (int partition : partitions) {
                            // 更新ZK节点
                            TopicState topicState = new TopicState(leaderEpoch, getControllerEpoch(), kafkaBroker.getId(), ids);
                            String content = JSON.toJSONString(topicState);
                            ZKUtils.updatePersistentPath(registry, ZKUtils.getTopicPartitionLeaderAndIsrPath(topic, partition),content);
                        }
                    }
                    // 通知controller更新节点
                    addPartitionUpdateEvent(topic, -1, KafkaClusterEvent.EventType.TOPIC_PARTITIONS_UPDATED, kafkaBroker.getId());
                }
            }
            localLastTopics = topicPartitions;
        }
    }

    private void addPartitionUpdateEvent(String topic, int partition, KafkaClusterEvent.EventType type, int lastBrokerId) {
        TopicsBrokerEvent topicsBrokerEvent = new TopicsBrokerEvent.Builder(topic, type).build();
        topicsBrokerEvent.setPartition(partition);
        topicsBrokerEvent.setLastBrokerId(lastBrokerId);
        topicsBrokerEvent.setControllerEpoch(getControllerEpoch());
        metadataUpdater.addPartitionUpdateEvent(topicsBrokerEvent);
    }

    /**
     * 更新Controller epoch
     * @param date
     */
    public void updateControllerEpoch(int date) {
        String path = ZKUtils.getControllerEpochPath();
        try {
            String controllerEpoch = String.valueOf(date);
            byte[] dateBytes = controllerEpoch.getBytes(Charsets.UTF_8);
            registry.update(path, dateBytes);
        } catch (RegistryException e) {
            logger.error(String.format("update persistent path %s error", path), e);
        }
    }

    /**
     * 获取本地broker
     */
    public KafkaBroker getKafkaBroker() {
        return kafkaBroker;
    }

    /**
     * 根据主题和partition查找对应的leader
     */
    public KafkaBroker getKafakBrokerOfPartition(String topic, int partition) {
        KafkaBroker kafkaBroker = null;
        // 从ZK节点中取出partition 对应leader信息
        byte[] statebytes = ZKUtils.getDataFromRegistry(registry, ZKUtils.getTopicPartitionLeaderAndIsrPath(topic, partition));
        if (statebytes != null && statebytes.length != 0) {
            String stateStr = new String(statebytes, Charsets.UTF_8);
            TopicState topicState = JSON.parseObject(stateStr, TopicState.class);
            if (topicState != null) {
                int leaderId = topicState.getLeader();
                kafkaBroker = metadataUpdater.metadataCache.getKafkaBroker(leaderId);
            }
        }
        return kafkaBroker;
    }

    /**
     * 获取Controller epoch
     * @return
     */
    public int getControllerEpoch() {
        int data = -1;
        String path = ZKUtils.getControllerEpochPath();

        byte[] bytes = ZKUtils.getDataFromRegistry(registry, path);
        if (bytes != null && bytes.length != 0) {
            String dataString = new String(bytes, Charsets.UTF_8);
            data = Integer.valueOf(dataString);
        } else {
            data = 0;
        }
        return data;
    }

    /**
     * 判断是否是主节点, 主节点负责数据更新
     *
     * @return
     */
    public boolean isMaster() {
        if (localBroker == null) {
            return false;
        }
        if (localBroker.getRole().equals(ClusterRole.MASTER)) {
            return true;
        }
        return false;
    }

    /**
     * 增加监听器
     *
     * @param listener 监听器
     */
    public void addListener(EventListener<KafkaClusterEvent> listener) {
        eventManager.addListener(listener);
    }

    /**
     * 移除监听器
     *
     * @param listener 监听器
     */
    public void removeListener(EventListener<KafkaClusterEvent> listener) {
        eventManager.removeListener(listener);
    }

    /**
     * 监听角色监听器角色变化
     */
    protected class RoleListener implements EventListener<RoleEvent> {

        @Override
        public void onEvent(RoleEvent roleEvent) {
            updateRoleChangePath(roleEvent);
        }
    }

    /**
     * 监听注册中心角色变化
     */
    protected class RoleChangeListener implements ChildrenDataListener {
        @Override
        public void onEvent(ChildrenEvent childrenEvent) {
            // 全量更新缓存
            updateKafkaTopicPartition();
        }
    }

    /**
     * 监听注册中心主题partition变化
     */
    protected class TopicPartitionChangeListener implements ChildrenDataListener {
        @Override
        public void onEvent(ChildrenEvent childrenEvent) {
            // 获取主题
            String path = childrenEvent.getPath();
            int topicIndexOf = path.lastIndexOf("/");
            topicIndexOf++;
            String topic = path.substring(topicIndexOf);
            metadataUpdater.updateTopicMetadataCache(topic);
        }
    }

    /**
     * 监听leader监听器
     */
    protected class KafkaLeaderListener implements LeaderListener {

        @Override
        public void onEvent(LeaderEvent event) {
            mutex.lock();
            try {
                if (!isStarted()) {
                    return;
                }
                if (event.getType() == LeaderEvent.LeaderEventType.TAKE) {
                    // take leader
                    logger.info(String.format("%s take leader", kafkaBroker.getHost()));
                    // 更新controller_epoch节点数据
                    int epoch = getControllerEpoch();
                    updateControllerEpoch(++epoch);
                    // 更新controller节点信息
                    Controller controller = new Controller();
                    controller.setBrokerid(kafkaBroker.getId());
                    controller.setTimestamp(SystemClock.now());
                    controller.setVersion(version);
                    String controllerBrokerStr = JSON.toJSONString(controller);
                    String path = ZKUtils.getControllerPath();
                    ZKUtils.updatePersistentPath(registry, path, controllerBrokerStr);
                } else if (event.getType() == LeaderEvent.LeaderEventType.LOST) {
                    // lost leader
                    logger.info(String.format("%s lost leader", kafkaBroker.getHost()));
                }
            } finally {
                mutex.unlock();
            }
        }
    }

    // 监听集群管理器事件通知
    protected class ClusterListener implements EventListener<com.ipd.jmq.server.broker.cluster.ClusterEvent> {

        @Override
        public void onEvent(com.ipd.jmq.server.broker.cluster.ClusterEvent event) {
            if (isStarted()) {
                try {
                    switch (event.getType()) {
                        case ALL_BROKER_UPDATE:
                            updateKafkaBroker();
                            break;
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    protected class TopicPartitionListener implements PathListener {
        @Override
        public void onEvent(PathEvent event) {
            if (isStarted()) {
                switch (event.getType()) {
                    case UPDATED:
                        // 更新主题partition信息
                        updateKafkaTopicPartition(event.getData());
                        // 更新缓存信息
                        metadataUpdater.updateMetadataCache();
                }
            }
        }
    }
}
