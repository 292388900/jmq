package com.ipd.kafka.coordinator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.HashMultimap;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.network.ClientConfig;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.kafka.command.*;
import com.ipd.jmq.common.network.kafka.exception.ErrorCode;
import com.ipd.jmq.common.network.kafka.exception.KafkaException;
import com.ipd.jmq.common.network.kafka.model.*;
import com.ipd.jmq.common.network.kafka.netty.KafkaNettyClient;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.registry.PathData;
import com.ipd.jmq.registry.Registry;
import com.ipd.jmq.registry.RegistryException;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
import com.ipd.jmq.toolkit.cache.CacheCluster;
import com.ipd.jmq.toolkit.concurrent.EventBus;
import com.ipd.jmq.toolkit.lang.Charsets;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.service.ServiceThread;
import com.ipd.jmq.toolkit.time.SystemClock;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.kafka.cluster.KafkaClusterManager;
import com.ipd.kafka.mapping.KafkaMapService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by zhangkepeng on 17-2-13.
 */
public class GroupMetadataManager {
    private final static Logger logger = LoggerFactory.getLogger(GroupMetadataManager.class);

    /* group metadata cache */
    private ConcurrentMap<String, GroupMetadata> groupsCache = new ConcurrentHashMap<String, GroupMetadata>();
    private BrokerConfig config;
    private KafkaClusterManager clusterManager;
    private DispatchService dispatchService;
    //private CacheCluster cacheService;
    private Registry registry;

    /* offset sync thread */
    private Thread thread;

    private EventBus<OffsetQueryEvent> queryEventBus;
    private OffsetQueryListener offsetQueryListener;

    private KafkaNettyClient kafkaNettyClient;
    // 查询offset请求clientId前缀
    private String clientIdPrefix = "query_offset";
    private static final String CONSUMER_OFFSETS = "/jmq/__consumer_offsets/";
    private int correlationId = 0;

    public GroupMetadataManager(BrokerConfig config, KafkaClusterManager clusterManager, DispatchService dispatchService) {
        this.config = config;
        this.clusterManager = clusterManager;
        this.dispatchService = dispatchService;
        //this.cacheService = config.getCacheService();
        this.registry = config.getRegistry();
        this.queryEventBus = new EventBus<OffsetQueryEvent>("OffsetQueryManager");
        this.offsetQueryListener = new OffsetQueryListener();
        if (kafkaNettyClient == null) {
            kafkaNettyClient = new KafkaNettyClient(new ClientConfig());
        }
    }

    /**
     * Get the group associated with the given groupId, or null if not found
     */
    protected GroupMetadata getGroup(String groupId) {
        return groupsCache.get(groupId);
    }

    protected void start() throws Exception {
        // start offset sync thread
        thread = new Thread(new OffsetTask(clusterManager), "OFFSET_SYNC");
        thread.start();
        queryEventBus.start();
        kafkaNettyClient.start();
        queryEventBus.addListener(offsetQueryListener);
    }

    protected void stop() {
        // close thread
        Close.close(thread).close(queryEventBus);
        thread = null;
        kafkaNettyClient.stop();
        queryEventBus.removeListener(offsetQueryListener);
    }

    protected boolean add(OffsetQueryEvent offsetQueryEvent) {
        return queryEventBus.add(offsetQueryEvent);
    }

    /**
     * Add a group or get the group associated with the given groupId if it already exists
     */
    protected GroupMetadata addGroup(GroupMetadata group) {
        //logger.info(System.nanoTime() + " | before put - Thread: " + Thread.currentThread().getName() + " - " + group);
        GroupMetadata currentGroup = groupsCache.putIfAbsent(group.getGroupId(), group);
        //logger.info(System.nanoTime() + " | after put - Thread: " + Thread.currentThread().getName() + " - " + currentGroup);
        if (currentGroup != null) {
            return currentGroup;
        } else {
            return group;
        }
    }

    /**
     * Remove all metadata associated with the group
     * @param group
     */
    protected void removeGroup(GroupMetadata group) {
        // guard this removal in case of concurrent access (e.g. if a delayed join completes with no members
        // while the group is being removed due to coordinator emigration)
        groupsCache.remove(group.getGroupId(), group);
    }

    protected OffsetCache prepareCacheOffset(final String groupId, final String memberId, final int generationId,
                                             final Map<String, Map<Integer, OffsetAndMetadata>> offsetMetadata, final GroupCoordinator.CommitCallback callback) {
        // cache to jimdb map data
        Map<TopicAndPartition, OffsetAndMetadata> cacheMap = new HashMap<TopicAndPartition, OffsetAndMetadata>();
        // first filter out partitions with offset metadata size exceeding limit
        Set<String> topics = offsetMetadata.keySet();
        if (topics != null && !topics.isEmpty()) {
            for (String topic : topics) {
                Map<Integer, OffsetAndMetadata> partitionOffsetMeta = offsetMetadata.get(topic);
                if (partitionOffsetMeta == null || partitionOffsetMeta.isEmpty()) {
                    continue;
                }
                Set<Integer> partitions = partitionOffsetMeta.keySet();
                if (partitions == null || partitions.isEmpty()) {
                    continue;
                }
                for (Integer partition : partitions) {
                    OffsetAndMetadata offsetAndMetadata = partitionOffsetMeta.get(partition);
                    if (offsetAndMetadata == null) {
                        continue;
                    }
                    if (validateOffsetMetadataLength(offsetAndMetadata.getMetadata())) {
                        cacheMap.put(new TopicAndPartition(topic, partition), offsetAndMetadata);
                    }
                }
            }
        }

        // call back func after caching
        GroupCoordinator.CommitCallback commitCallback = new GroupCoordinator.CommitCallback() {
            @Override
            public void sendResponseCallback(Map<String, Map<Integer, Short>> commitStatus) {
                Set<String> topics = offsetMetadata.keySet();
                if (topics != null && !topics.isEmpty()) {
                    for (String topic : topics) {
                        if (!commitStatus.containsKey(topic)) {
                            commitStatus.put(topic, new HashMap<Integer, Short>());
                        }
                        Map<Integer, Short> partitionStatus = commitStatus.get(topic);
                        Map<Integer, OffsetAndMetadata> partitionOffsetMeta = offsetMetadata.get(topic);
                        // 之前去掉metadata不合法的，所有partitionOffsetMeta数量不小于partitionStatus
                        if (partitionOffsetMeta == null || partitionOffsetMeta.isEmpty()) {
                            continue;
                        }
                        Set<Integer> partitions = partitionOffsetMeta.keySet();
                        if (partitions == null || partitions.isEmpty()) {
                            continue;
                        }
                        for (Integer partition : partitions) {
                            OffsetAndMetadata offsetAndMetadata = partitionOffsetMeta.get(partition);
                            if (offsetAndMetadata == null) {
                                continue;
                            }
                            if (!validateOffsetMetadataLength(offsetAndMetadata.getMetadata())) {
                                partitionStatus.put(partition, ErrorCode.OFFSET_METADATA_TOO_LARGE);
                            }
                        }
                    }
                }
                callback.sendResponseCallback(commitStatus);
            }
        };

        return new OffsetCache(groupId, cacheMap, commitCallback);
    }

    protected void cacheOffset(OffsetCache offsetCache) {
        Map<String, Map<Integer, Short>> commitStatus = new HashMap<String, Map<Integer, Short>>();
        Map<TopicAndPartition, OffsetAndMetadata> offsetAndMetadatas = offsetCache.cacheMap;
        if (offsetAndMetadatas != null && !offsetAndMetadatas.isEmpty()) {
            Set<TopicAndPartition> topicAndPartitions = offsetAndMetadatas.keySet();
            for (TopicAndPartition topicAndPartition : topicAndPartitions) {
                String cacheKeyStr = CacheKey.OFFSET_ID.format(topicAndPartition.getTopic(), offsetCache.groupId, topicAndPartition.getPartition());
                OffsetAndMetadata offsetAndMetadata = offsetAndMetadatas.get(topicAndPartition);
                if (offsetAndMetadata.getOffsetCacheRetainTime() == -1L) {
                    offsetAndMetadata.setOffsetCacheRetainTime(config.getOffsetCacheRetainTime());
                }
                int retainTime = (int)(offsetAndMetadata.getOffsetCacheRetainTime() / 1000L);
                String cacheValueStr = JSONObject.toJSONString(offsetAndMetadata);
                //cacheService.setex(cacheKeyStr, retainTime, cacheValueStr);
                try {
                    registry.update(CONSUMER_OFFSETS + cacheKeyStr, cacheValueStr.getBytes(Charsets.UTF_8));
                } catch (RegistryException e) {
                    logger.error(e.getMessage(), e);
                }
                Map<Integer, Short> partitionStatus = commitStatus.get(topicAndPartition.getTopic());
                if (partitionStatus == null) {
                    partitionStatus = new HashMap<Integer, Short>();
                    partitionStatus.put(topicAndPartition.getPartition(), ErrorCode.NO_ERROR);
                    commitStatus.put(topicAndPartition.getTopic(), partitionStatus);
                } else {
                    partitionStatus.put(topicAndPartition.getPartition(), ErrorCode.NO_ERROR);
                }
            }
        }
        offsetCache.commitCallback.sendResponseCallback(commitStatus);
    }

    protected static class OffsetCache {
        private GroupCoordinator.CommitCallback commitCallback;
        private String groupId;
        private Map<TopicAndPartition, OffsetAndMetadata> cacheMap;

        public OffsetCache(String groupId, Map<TopicAndPartition, OffsetAndMetadata> cacheMap, GroupCoordinator.CommitCallback commitCallback) {
            this.groupId = groupId;
            this.cacheMap = cacheMap;
            this.commitCallback = commitCallback;
        }
    }

    protected static class OffsetQueryEvent {
        private GroupCoordinator.QueryCallback queryCallback;
        private Map<String, Map<Integer, OffsetRequest.PartitionOffsetRequestInfo>> offsetRequestInfo;

        public OffsetQueryEvent(Map<String, Map<Integer, OffsetRequest.PartitionOffsetRequestInfo>> offsetRequestInfo, GroupCoordinator.QueryCallback queryCallback) {
            this.offsetRequestInfo = offsetRequestInfo;
            this.queryCallback = queryCallback;
        }
    }

    protected class OffsetQueryListener implements EventListener<OffsetQueryEvent> {

        @Override
        public void onEvent(OffsetQueryEvent kafkaClusterEvent) {
            if (kafkaClusterEvent == null) {
                return;
            }
            Map<String, Transport> transports = new HashMap<String, Transport>();
            Map<String, Map<Integer, PartitionOffsetsResponse>> topicPartitionOffsetInfo = new HashMap<String, Map<Integer, PartitionOffsetsResponse>>();
            GroupCoordinator.QueryCallback queryCallback = kafkaClusterEvent.queryCallback;
            Map<String, Map<Integer, OffsetRequest.PartitionOffsetRequestInfo>> offsetRequestInfo = kafkaClusterEvent.offsetRequestInfo;
            Set<String> topics = offsetRequestInfo.keySet();
            try {
                for (String topic : topics) {
                    // 从请求命令中根据主题获取partition->OffsetRequest.PartitionOffsetRequestInfo
                    Map<Integer, OffsetRequest.PartitionOffsetRequestInfo> partitionOffsetRequestMap = offsetRequestInfo.get(topic);
                    Set<Integer> partitions = partitionOffsetRequestMap.keySet();
                    // 根据主题和partition分别获取对应的offset
                    for (int partition : partitions) {
                        OffsetRequest.PartitionOffsetRequestInfo partitionOffsetRequestInfo = partitionOffsetRequestMap.get(partition);
                        long timestamp = partitionOffsetRequestInfo.getTime();
                        int maxNumOffsets = partitionOffsetRequestInfo.getMaxNumOffsets();
                        KafkaBroker kafkaBroker = clusterManager.getKafakBrokerOfPartition(topic, partition);
                        PartitionOffsetsResponse partitionOffsetsResponse = null;
                        if (kafkaBroker != null) {
                            Transport transport = transports.get(kafkaBroker.toString());
                            //与broker建立连接，1秒钟连接超时
                            try {
                                if (transport == null) {
                                    //构造Broker地址
                                    InetSocketAddress brokerAddress = new InetSocketAddress(kafkaBroker.getHost(), kafkaBroker.getPort());
                                    transport = kafkaNettyClient.createTransport(brokerAddress, 1000L);
                                    transports.put(kafkaBroker.toString(), transport);
                                }
                                partitionOffsetsResponse = sendQueryOffsetCommand(transport, topic, partition, timestamp, maxNumOffsets);
                            } catch (Exception e) {
                                logger.error(e.getMessage(), e);
                            }
                        }

                        if (partitionOffsetsResponse == null) {
                            Set<Long> offset = new HashSet<Long>();
                            partitionOffsetsResponse = new PartitionOffsetsResponse(ErrorCode.UNKNOWN, offset);
                        }
                        Map<Integer, PartitionOffsetsResponse> partitionOffsetsResponseMap = new HashMap<Integer, PartitionOffsetsResponse>();
                        partitionOffsetsResponseMap.put(partition, partitionOffsetsResponse);
                        // 主题下所有partition对应的PartitionOffsetsResponse
                        topicPartitionOffsetInfo.put(topic, partitionOffsetsResponseMap);
                    }
                }
            } finally {
                if (!transports.isEmpty()) {
                    Collection<Transport> transportSet = transports.values();
                    for (Transport transport : transportSet) {
                        transport.stop();
                    }
                }
            }
            queryCallback.sendResponseCallback(topicPartitionOffsetInfo);
        }
    }

    protected enum CacheKey {

        //OFFSET_ID("topic_groupId_partition||%s||%s||%d");
        OFFSET_ID("_%s_%s_%d");

        private String format;

        CacheKey(String format) {
            this.format = format;
        }

        public String format(Object... values) {
            return String.format(format, values);
        }

    }

    protected PartitionOffsetsResponse sendQueryOffsetCommand(Transport transport, String topic, int partition,
                                                              long timestamp, int maxNumOffsets) throws Exception{
        KafkaCommandFactory kafkaCommandFactory = new KafkaCommandFactory();
        KafkaHeader kafkaHeader = KafkaHeader.Builder.request(KafkaCommandKeys.OFFSET_QUERY);
        kafkaHeader.setCommandKey(KafkaCommandKeys.OFFSET_QUERY);
        Command command = kafkaCommandFactory.create(kafkaHeader);
        OffsetQueryRequest payload = (OffsetQueryRequest)command.getPayload();
        if (payload != null) {
            // 构造请求
            String clientId = clientIdPrefix + SystemClock.now();
            correlationId++;
            payload.setVersion((short)1);
            payload.setCorrelationId(correlationId);
            payload.setClientId(clientId);
            payload.setTopic(topic);
            payload.setPartition(partition);
            payload.setTimestamp(timestamp);
            payload.setMaxNumOffsets(maxNumOffsets);
            command.setPayload(payload);
        } else {
            throw new KafkaException("not supported command type");
        }
        //logger.info("1 : " + kafkaHeader.getRequestId());
        Command ack = transport.sync(command, 5000);
        OffsetQueryResponse offsetQueryResponse = (OffsetQueryResponse)ack.getPayload();
        if (offsetQueryResponse != null) {
            if (correlationId == offsetQueryResponse.getCorrelationId()) {
                PartitionOffsetsResponse partitionOffsetsResponse = offsetQueryResponse.getPartitionOffsetsResponse();
                return partitionOffsetsResponse;
            }
            return null;
        } else {
            return null;
        }
    }

    /*
   * Check if the offset metadata length is valid
   */
    private boolean validateOffsetMetadataLength(String metadata) {
        return metadata == null || metadata.length() <= config.getMaxMetadataSize();
    }

    protected Map<String, Map<Integer, OffsetMetadataAndError>> getOffsets(String groupId, HashMultimap<String, Integer> topicAndPartitions) {

        Map<String, Map<Integer, OffsetMetadataAndError>> topicPartitionOffsetMetadataMap = new HashMap<String, Map<Integer, OffsetMetadataAndError>>();
        if (topicAndPartitions == null || topicAndPartitions.isEmpty()) {
            return topicPartitionOffsetMetadataMap;
        }
        Set<String> topics = topicAndPartitions.keySet();
        for (String topic : topics) {
            Set<Integer> partitions = topicAndPartitions.get(topic);
            Map<Integer, OffsetMetadataAndError> partitionOffsetMetadataMap = new HashMap<Integer, OffsetMetadataAndError>();
            for (int partition : partitions) {
                OffsetMetadataAndError offsetMetadataAndError = getOffsetMetadataAndError(topic, groupId, partition);
                partitionOffsetMetadataMap.put(partition, offsetMetadataAndError);
            }
            topicPartitionOffsetMetadataMap.put(topic, partitionOffsetMetadataMap);
        }
        return topicPartitionOffsetMetadataMap;
    }

    private OffsetMetadataAndError getOffsetMetadataAndError(String topic, String groupId, int partition) {
        // cache offset及partition信息无需映射为JMQ
        String cacheKeyStr = CacheKey.OFFSET_ID.format(topic, groupId, partition);
        //logger.info(cacheKeyStr);
        //String cacheValueStr = cacheService.get(cacheKeyStr);
        String cacheValueStr = null;
        try {
            PathData pathData = registry.getData(CONSUMER_OFFSETS + cacheKeyStr);
            cacheValueStr = null == pathData || null == pathData.getData() ? null : new String(pathData.getData());
        } catch (RegistryException e) {
            logger.error(e.getMessage(), e);
        }
        OffsetMetadataAndError offsetMetadataAndError;
        if (cacheValueStr == null) {
            // 消费位置不存在，返回错误，从新获取offset
            offsetMetadataAndError = new OffsetMetadataAndError(OffsetAndMetadata.INVALID_OFFSET, OffsetAndMetadata.NO_METADATA, ErrorCode.NO_ERROR);
        } else {
            OffsetAndMetadata offsetAndMetadata = JSONObject.parseObject(cacheValueStr, OffsetAndMetadata.class);
            offsetMetadataAndError = new OffsetMetadataAndError(offsetAndMetadata.getOffset(), offsetAndMetadata.getMetadata(), ErrorCode.NO_ERROR);
            //logger.info("get offsets success : " + offsetAndMetadata.getOffset());
        }
        return offsetMetadataAndError;
    }

    /**
     * offset sync task
     */
    protected class OffsetTask extends ServiceThread {

        public OffsetTask(Service parent) {
            super(parent);
        }

        @Override
        protected void execute() throws Exception {
            // 不需要加锁，定期同步offset到broker，过期后不进行同步
            Map<String, Set<Integer>> topicGroupPartitions = clusterManager.getLocalLastTopics();
            Set<String> topics = topicGroupPartitions.keySet();
            if (topics != null) {
                for (String topic : topics) {
                    Set<Integer> partitions = topicGroupPartitions.get(topic);
                    List<String> groupIds = clusterManager.getConsumerApp(topic);
                    if (groupIds != null && !groupIds.isEmpty()) {
                        for (String groupId : groupIds) {
                            for (Integer partition : partitions) {
                                String cacheKeyStr = CacheKey.OFFSET_ID.format(topic, groupId, partition);
                                //String cacheValueStr = cacheService.get(cacheKeyStr);
                                String cacheValueStr = null;
                                try {
                                    PathData pathData = registry.getData(CONSUMER_OFFSETS + cacheKeyStr);
                                    cacheValueStr = null == pathData || null == pathData.getData() ? null : new String(pathData.getData());
                                } catch (RegistryException e) {
                                    logger.error(e.getMessage(), e);
                                }
                                OffsetAndMetadata offsetAndMetadata = null;
                                if (cacheValueStr != null) {
                                    offsetAndMetadata = JSON.parseObject(cacheValueStr, OffsetAndMetadata.class);
                                    commitOffsetAndMetadata(topic, groupId, partition, offsetAndMetadata);
                                }
                            }
                        }
                    }
                }
            }
        }

        /**
         * 提交offset，返回提交状态
         *
         * @param topic
         * @param groupId
         * @param partition
         * @param offsetAndMetadata
         * @return
         */
        private short commitOffsetAndMetadata(String topic, String groupId, int partition, OffsetAndMetadata offsetAndMetadata) {
            short status = ErrorCode.NO_ERROR;
            short queue = KafkaMapService.partition2Queue(partition);
            long offset = KafkaMapService.map2Offset(offsetAndMetadata.getOffset(), KafkaMapService.KAFKA_OFFSET_TO_JMQ_OFFSET);
            try {
                dispatchService.resetAckOffset(topic, queue, groupId, offset);
            } catch (JMQException e) {
                status = ErrorCode.codeFor(e.getCode());
            }
            return status;
        }

        @Override
        public long getInterval() {
            return config.getOffsetSyncInterval();
        }

        @Override
        public boolean onException(Throwable e) {
            logger.error("offset sync error.", e);
            return true;
        }
    }
}