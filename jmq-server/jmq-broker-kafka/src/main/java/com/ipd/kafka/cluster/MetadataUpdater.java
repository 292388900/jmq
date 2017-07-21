package com.ipd.kafka.cluster;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ipd.jmq.common.network.kafka.command.*;
import com.ipd.jmq.common.network.kafka.model.*;
import com.ipd.jmq.common.network.kafka.exception.ErrorCode;
import com.ipd.jmq.common.network.kafka.exception.KafkaException;
import com.ipd.jmq.common.network.kafka.exception.LeaderNotAvailableException;
import com.ipd.jmq.common.network.kafka.netty.KafkaNettyClient;
import com.ipd.jmq.common.network.kafka.utils.ZKUtils;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.common.network.ClientConfig;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.registry.Registry;
import com.ipd.jmq.registry.RegistryException;
import com.ipd.jmq.toolkit.concurrent.EventBus;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.lang.Charsets;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by zhangkepeng on 16-8-8.
 *
 */
public class MetadataUpdater extends Service {
    private final static Logger logger = LoggerFactory.getLogger(MetadataUpdater.class);

    // 分片配置
    private BrokerConfig brokerConfig;
    // 注册中心
    private Registry registry;
    // kafka集群管理
    private KafkaClusterManager kafkaClusterManager;
    // netty客户端
    private KafkaNettyClient kafkaNettyClient;
    // 更新主题broker信息客户端ID
    private String clientId = "update_topic_brokers";
    // 更新主题broker信息correlationId
    private int correlationId = 0;
    // 更新缓存线程池
    private ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 5, 0, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(500));


    // 集群事件管理器
    private EventBus<KafkaClusterEvent> eventManager;
    // 集群更新监听器
    private EventListener<KafkaClusterEvent> kafkaClusterEventListener = new KafkaClusterListener();

    // 缓存锁
    private static final ReentrantReadWriteLock partitionMetadataLock = new ReentrantReadWriteLock();
    private static final Lock readLock = partitionMetadataLock.readLock();
    private static final Lock writeLock = partitionMetadataLock.writeLock();

    // 元数据缓存
    public static MetadataCache metadataCache = new MetadataCache();

    public MetadataUpdater(EventBus<KafkaClusterEvent> eventManager, BrokerConfig brokerConfig, KafkaClusterManager kafkaClusterManager) {
        this.eventManager = eventManager;
        this.brokerConfig = brokerConfig;
        this.kafkaClusterManager = kafkaClusterManager;
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        Preconditions.checkArgument(eventManager != null, "EventBus can not be null");
        Preconditions.checkArgument(brokerConfig != null, "MetadataCache can not be null");
        this.registry = brokerConfig.getRegistry();
        Preconditions.checkArgument(registry != null, "Registry can not be null");
        if (kafkaNettyClient == null) {
            kafkaNettyClient = new KafkaNettyClient(new ClientConfig());
        }
    }

    @Override
    public void doStart() throws Exception {
        super.doStart();
        kafkaNettyClient.start();
        eventManager.addListener(kafkaClusterEventListener);
    }

    @Override
    public void doStop() {
        super.doStop();
        kafkaNettyClient.stop();
        eventManager.removeListener(kafkaClusterEventListener);
        executor.shutdown();
        metadataCache.clear();
    }

    /**
     * 添加partition更新事件
     */
    public short addPartitionUpdateEvent(KafkaClusterEvent kafkaClusterEvent) {
        boolean isSuccess = eventManager.add(kafkaClusterEvent);
        if (!isSuccess) {
            return ErrorCode.UNKNOWN;
        }
        return ErrorCode.NO_ERROR;
    }

    protected class KafkaClusterListener implements EventListener<KafkaClusterEvent> {

        @Override
        public void onEvent(KafkaClusterEvent kafkaClusterEvent) {
            if (!isStarted()) {
                return;
            }
            TopicsBrokerEvent topicsBrokerEvent = (TopicsBrokerEvent) kafkaClusterEvent;
            if (!ZKUtils.isLeader(registry)) {
                // 发送命令到Controller，失败会重试三次
                int retryTimes = 0;
                while (retryTimes < 3) {
                    short status = sendCommand2Controller(topicsBrokerEvent);
                    if (status == ErrorCode.NO_ERROR) {
                        break;
                    } else {
                        retryTimes++;
                    }
                }
            } else {
                int controllerEpoch = topicsBrokerEvent.getControllerEpoch();
                int zkControllerEpoch = kafkaClusterManager.getControllerEpoch();
                if (controllerEpoch == zkControllerEpoch) {
                    // topics下主题节点数据更新，Controller进行写数据
                    String topic = topicsBrokerEvent.getTopic();
                    int partition = topicsBrokerEvent.getPartition();
                    int topicsBrokerType = topicsBrokerEvent.getType().type;
                    int lastBrokerId = topicsBrokerEvent.getLastBrokerId();
                    try {
                        if (topicsBrokerType == KafkaClusterEvent.EventType.PARTITION_REMOVED.getType()) {
                            String childPath = ZKUtils.getTopicPartitionLeaderAndIsrPath(topic, partition);
                            String content = ZKUtils.getDataPersistentPath(registry, childPath);
                            if (content != null) {
                                TopicState topicState = JSON.parseObject(content, TopicState.class);
                                int leader = topicState.getLeader();
                                if (leader == lastBrokerId) {
                                    // ZK上对应的leader没有变化，是需要删除的节点
                                    ZKUtils.deletePersistentPath(registry, childPath);
                                    String deletePath = ZKUtils.getTopicPartitionPath(topic, partition);
                                    ZKUtils.deletePersistentPath(registry, deletePath);
                                }
                            }
                            updateTopicPartitions(topic);
                        } else if (topicsBrokerType == KafkaClusterEvent.EventType.TOPIC_PARTITIONS_UPDATED.getType()) {
                            // controller更新主题下的分片信息
                            updateTopicPartitions(topic);
                        }
                    } catch (RegistryException e) {
                        logger.error("update brokers topics topic data exception");
                    }
                }
            }
        }
    }

    private void updateTopicPartitions(String topic) throws RegistryException{
        Map<String, Set<Integer>> partitionAndIsr = new HashMap<String, Set<Integer>>();
        String path = ZKUtils.getTopicPartitionsPath(topic);
        List<String> partitions = registry.getChildren(path);
        for (String currentPartition : partitions) {
            int partitionNode = Integer.valueOf(currentPartition);
            String childPath = ZKUtils.getTopicPartitionLeaderAndIsrPath(topic, partitionNode);
            String content = ZKUtils.getDataPersistentPath(registry, childPath);
            if (content == null) {
                continue;
            }
            TopicState topicState = JSON.parseObject(content, TopicState.class);
            Set<Integer> isr = topicState.getIsr();
            partitionAndIsr.put(currentPartition, isr);
        }
        TopicsBroker topicsBroker = new TopicsBroker();
        topicsBroker.setPartitions(partitionAndIsr);

        String content = JSON.toJSONString(topicsBroker);
        // 更新ZK节点数据
        ZKUtils.updatePersistentPath(registry, ZKUtils.getTopicPath(topic), content);
    }

    /**
     * 发送命令给controller
     *
     * @param topicsBrokerEvent
     * @return
     */
    private short sendCommand2Controller(TopicsBrokerEvent topicsBrokerEvent) {
        String controllerHost = null;
        int port = 0;
        // 从controller节点中取出controller信息
        byte[] bytes = ZKUtils.getDataFromRegistry(registry, ZKUtils.getControllerPath());
        if (bytes != null && bytes.length != 0) {
            String controllerStr = new String(bytes, Charsets.UTF_8);
            Controller controller = JSON.parseObject(controllerStr, Controller.class);
            int brokerId = controller.getBrokerid();
            if (brokerId == KafkaClusterManager.initValue) {
                logger.error("controller not available");
                return ErrorCode.UNKNOWN;
            }
            // 先从缓存中获取controller信息
            KafkaBroker kafkaBroker = metadataCache.getKafkaBroker(brokerId);
            if (kafkaBroker != null) {
                controllerHost = kafkaBroker.getHost();
                port = kafkaBroker.getPort();
            } else {
                // 从ZK节点中取出controller信息
                byte[] controllerBytes = ZKUtils.getDataFromRegistry(registry, ZKUtils.getBrokerIdsPath(brokerId));
                if (controllerBytes != null && controllerBytes.length != 0) {
                    String controllerBrokerStr = new String(controllerBytes, Charsets.UTF_8);
                    KafkaLiveBroker controllerBroker = JSON.parseObject(controllerBrokerStr, KafkaLiveBroker.class);
                    if (controllerBroker != null) {
                        controllerHost = controllerBroker.getHost();
                        port = controllerBroker.getPort();
                    }
                }
            }
        }
        if (controllerHost == null) {
            logger.error("controller host null and send UpdateTopicsBrokerRequest command to controller error");
            return ErrorCode.UNKNOWN;
        }
        short status = sendUpdateTopicsCommand(controllerHost, port, topicsBrokerEvent);
        return status;
    }

    /**
     * 创建通道同步发送更新命令
     * @param host
     * @param port
     */
    private short sendUpdateTopicsCommand(String host, int port, TopicsBrokerEvent topicsBrokerEvent) {
        Transport transport = null;
        SocketAddress brokerAddress;
        KafkaCommandFactory kafkaCommandFactory = new KafkaCommandFactory();
        try {
            //构造Broker地址
            brokerAddress = new InetSocketAddress(host, port);
            //与broker建立连接，1秒钟连接超时
            transport = kafkaNettyClient.createTransport(brokerAddress, 1000L);
            KafkaHeader kafkaHeader = KafkaHeader.Builder.request(KafkaCommandKeys.UPDATE_TOPICS_BROKER);
            kafkaHeader.setCommandKey(KafkaCommandKeys.UPDATE_TOPICS_BROKER);
            Command command = kafkaCommandFactory.create(kafkaHeader);
            // 同步发送更新命令给Controller
            UpdateTopicsBrokerRequest payload = (UpdateTopicsBrokerRequest)command.getPayload();
            if (payload != null) {
                String topic = topicsBrokerEvent.getTopic();
                int partition = topicsBrokerEvent.getPartition();
                int topicsBrokerType = topicsBrokerEvent.getType().type;
                int lastBrokerId = topicsBrokerEvent.getLastBrokerId();
                int controllerEpoch = topicsBrokerEvent.getControllerEpoch();
                // 构造请求
                clientId += SystemClock.now();
                correlationId++;
                payload.setClientId(clientId);
                payload.setTopic(topic);
                payload.setPartition(partition);
                payload.setTopicsBrokerType(topicsBrokerType);
                payload.setLastBrokerId(lastBrokerId);
                payload.setControllerEpoch(controllerEpoch);
                payload.setCorrelationId(correlationId);
                payload.setVersion((short)1);
                command.setPayload(payload);
            } else {
                throw new KafkaException("not supported command type");
            }
            Command ack = transport.sync(command, 5000);
            UpdateTopicsBrokerResponse updateTopicsBrokerResponse = (UpdateTopicsBrokerResponse)ack.getPayload();
            if (updateTopicsBrokerResponse != null) {
                if (correlationId == updateTopicsBrokerResponse.getCorrelationId()) {
                    short status = updateTopicsBrokerResponse.getErrorCode();
                    return status;
                }
            } else {
                return ErrorCode.UNKNOWN;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (transport != null) {
                transport.stop();
            }
        }
        return ErrorCode.UNKNOWN;
    }

    /**
     * 更新缓存元数据信息
     */
    public void updateMetadataCache() {
        String topicsPath = ZKUtils.brokerTopicsPath;
        List<String> topics = ZKUtils.getChildren(registry, topicsPath);

        if (topics != null && !topics.isEmpty()) {
            for (String topic : topics) {
                updateTopicMetadataCache(topic);
            }
        }
    }

    /**
     * 更新主题的缓存数据
     */
    public void updateTopicMetadataCache(final String topic) {
        final String topicPath = ZKUtils.getTopicPath(topic);
        final String partitionPath = ZKUtils.getTopicPartitionsPath(topic);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                if (registry != null) {
                    if (ZKUtils.isExists(registry, topicPath)) {
                        List<String> nodeList = ZKUtils.getChildren(registry, partitionPath);
                        if (nodeList == null || nodeList.isEmpty()) {
                            ZKUtils.deletePersistentPath(registry, ZKUtils.getTopicPartitionsPath(topic));
                            ZKUtils.deletePersistentPath(registry, ZKUtils.getTopicPath(topic));
                            metadataCache.getCache().remove(topic);
                            return;
                        }
                        String topicsBrokerStr = ZKUtils.getDataPersistentPath(registry, topicPath);
                        if (StringUtils.isNoneEmpty(topicsBrokerStr)) {
                            TopicsBroker topicsBroker = JSONObject.parseObject(topicsBrokerStr, TopicsBroker.class);
                            // 从缓存中获取主题所有的partition，删除无效partition
                            Set<Integer> cachePartitions = null;
                            Map<Integer, PartitionStateInfo> partitionStateInfoMap = metadataCache.getCache().get(topic);
                            if (partitionStateInfoMap != null) {
                                cachePartitions = partitionStateInfoMap.keySet();
                            }
                            Map<String, Set<Integer>> partitionsMap = topicsBroker.getPartitions();
                            Set<String> partitionStrs = partitionsMap.keySet();
                            // 从ZK上获取的主题partition信息
                            Set<Integer> partitions = new HashSet<Integer>();
                            if (partitionStrs != null) {
                                for (String partitionStr : partitionStrs) {
                                    int partitionNode = Integer.valueOf(partitionStr);
                                    partitions.add(partitionNode);
                                }
                            }

                            Set<Integer> tmpPartitions = new HashSet<Integer>();
                            if (cachePartitions != null && !cachePartitions.isEmpty()) {
                                tmpPartitions.addAll(cachePartitions);
                            }
                            if (!partitions.isEmpty()) {
                                tmpPartitions.removeAll(partitions);
                            }
                            for (int deletePartiton : tmpPartitions) {
                                metadataCache.removePartitionInfo(topic, deletePartiton);
                            }

                            for (int partition : partitions) {
                                // 所有数据都从zk获取最小信息
                                String childPath = ZKUtils.getTopicPartitionLeaderAndIsrPath(topic, partition);
                                String content = ZKUtils.getDataPersistentPath(registry, childPath);
                                TopicState topicState = JSON.parseObject(content, TopicState.class);
                                int leader = topicState.getLeader();
                                Set<Integer> isr = topicState.getIsr();

                                PartitionStateInfo partitionStateInfo = new PartitionStateInfo();
                                LeaderIsr leaderIsr = new LeaderIsr(leader, KafkaClusterManager.leaderEpoch, isr, 1);
                                partitionStateInfo.setLeaderIsr(leaderIsr);
                                partitionStateInfo.setReplicas(isr);
                                metadataCache.addOrUpdatePartitionInfo(topic, partition, partitionStateInfo);
                            }
                        }
                    }
                }
            }
        });
    }

    public static class MetadataCache {

        private Map<String, Map<Integer, PartitionStateInfo>> cache = new HashMap<String, Map<Integer, PartitionStateInfo>>();

        private Map<Integer, KafkaBroker> aliveBrokers = new HashMap<Integer, KafkaBroker>();

        public Set<KafkaBroker> getKafkaBrokers() {
            readLock.lock();
            Set<KafkaBroker> kafkaBrokers = new HashSet<KafkaBroker>();
            try {
                for (KafkaBroker kafkaBroker : aliveBrokers.values()) {
                    kafkaBrokers.add(kafkaBroker);
                }
            } finally {
                readLock.unlock();
            }
            return kafkaBrokers;
        }

        public KafkaBroker getKafkaBroker(int brokerId) {
            if (!aliveBrokers.isEmpty()) {
                return aliveBrokers.get(brokerId);
            }
            return null;
        }

        public Map<String, Map<Integer, PartitionStateInfo>> getCache() {
            readLock.lock();
            try {
                return cache;
            } finally {
                readLock.unlock();
            }
        }

        public void setAliveBrokers(Map<Integer, KafkaBroker> kafkaBrokers) {
            writeLock.lock();
            try {
                aliveBrokers.clear();
                aliveBrokers = kafkaBrokers;
            } finally {
                writeLock.unlock();
            }
        }

        public List<TopicMetadata> getTopicMetadata(Set<String> topics) {
            Set<String> topicsRequested = topics;
            if (topics == null || topics.isEmpty()) {
                // 参数为空，拿取缓存中所有主题
                topicsRequested = cache.keySet();
            }
            List<TopicMetadata> topicResponses = new ArrayList<TopicMetadata>();
            readLock.lock();
            try {
                for (String topic : topicsRequested) {
                    Map<Integer, PartitionStateInfo> stateInfoMap = cache.get(topic);
                    if (stateInfoMap != null) {
                        List<PartitionMetadata> partitionMetadatas = new ArrayList<PartitionMetadata>();
                        Iterator<Map.Entry<Integer, PartitionStateInfo>> stateInfos = stateInfoMap.entrySet().iterator();
                        while (stateInfos.hasNext()) {
                            Map.Entry<Integer, PartitionStateInfo> stateInfoEntry = stateInfos.next();
                            int partitionId = stateInfoEntry.getKey();
                            PartitionStateInfo partitionStateInfo = stateInfoEntry.getValue();
                            Set<Integer> replicas = partitionStateInfo.getReplicas();
                            Set<KafkaBroker> replicaBrokers = new HashSet<KafkaBroker>();
                            Set<KafkaBroker> isrInfos = new HashSet<KafkaBroker>();
                            for (int replica : replicas) {
                                KafkaBroker kafkaBroker = aliveBrokers.get(replica);
                                if (kafkaBroker != null) {
                                    replicaBrokers.add(kafkaBroker);
                                }
                            }
                            KafkaBroker leaderInfo = null;
                            LeaderIsr leaderIsr = partitionStateInfo.getLeaderIsr();
                            int leader = leaderIsr.getLeader();
                            Set<Integer> isrs = leaderIsr.getIsr();
                            TopicAndPartition topicPartition = new TopicAndPartition(topic, partitionId);
                            try {
                                leaderInfo = aliveBrokers.get(leader);
                                if (leaderInfo == null) {
                                    throw new LeaderNotAvailableException(String.format("Leader not available for %s", topicPartition));
                                }
                                for (int isr : isrs) {
                                    KafkaBroker isrKafkaBroker = aliveBrokers.get(isr);
                                    if (isrKafkaBroker != null) {
                                        isrInfos.add(isrKafkaBroker);
                                    }
                                }
                                partitionMetadatas.add(new PartitionMetadata(partitionId, leaderInfo, replicaBrokers, isrInfos, ErrorCode.NO_ERROR));
                            } catch (Throwable e) {
                                logger.debug(String.format("Error while fetching metadata for %s. Possible cause: %s", topicPartition, e.getMessage()));
                                partitionMetadatas.add(new PartitionMetadata(partitionId, leaderInfo, replicaBrokers, isrInfos, ErrorCode.UNKNOWN));
                            }

                        }
                        topicResponses.add(new TopicMetadata(topic, partitionMetadatas, ErrorCode.NO_ERROR));
                    }
                }
            } finally {
                readLock.unlock();
            }
            return topicResponses;
        }

        public void addOrUpdatePartitionInfo(String topic, int partition, PartitionStateInfo partitionStateInfo) {
            writeLock.lock();
            try {
                Map<Integer, PartitionStateInfo> partitionStateInfoMap = cache.get(topic);
                if (partitionStateInfoMap == null) {
                    Map<Integer, PartitionStateInfo> partitionStateInfos = new HashMap<Integer, PartitionStateInfo>();
                    partitionStateInfos.put(partition, partitionStateInfo);
                    cache.put(topic, partitionStateInfos);
                } else {
                    partitionStateInfoMap.put(partition, partitionStateInfo);
                }
            } finally {
                writeLock.unlock();
            }
        }

        public boolean removePartitionInfo(String topic, int partitionId) {
            writeLock.lock();
            try {
                Map<Integer, PartitionStateInfo> partitionStateInfoMap = cache.get(topic);
                if (partitionStateInfoMap != null) {
                    PartitionStateInfo partitionStateInfo = partitionStateInfoMap.get(partitionId);
                    if (partitionStateInfo != null) {
                        partitionStateInfoMap.remove(partitionId);
                        if (partitionStateInfoMap.isEmpty()) {
                            cache.remove(topic);
                        }
                        return true;
                    }
                }
                return false;
            } finally {
                writeLock.unlock();
            }
        }

        public void clear() {
            writeLock.lock();
            try {
                cache.clear();
                aliveBrokers.clear();
            } finally {
                writeLock.unlock();
            }
        }
    }
}
