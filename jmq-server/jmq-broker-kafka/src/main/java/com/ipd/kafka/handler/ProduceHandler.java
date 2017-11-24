package com.ipd.kafka.handler;

import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.MessageLocation;
import com.ipd.jmq.common.message.MessageQueue;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaHeader;
import com.ipd.jmq.common.network.kafka.command.ProduceRequest;
import com.ipd.jmq.common.network.kafka.command.ProduceResponse;
import com.ipd.jmq.common.network.kafka.exception.ErrorCode;
import com.ipd.jmq.common.network.kafka.message.ByteBufferMessageSet;
import com.ipd.jmq.common.network.kafka.model.ProducerPartitionStatus;
import com.ipd.jmq.common.network.v3.command.*;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.handler.AbstractHandler;
import com.ipd.jmq.server.broker.handler.PutMessageHandler;
import com.ipd.jmq.server.store.PutResult;
import com.ipd.kafka.cluster.KafkaClusterManager;
import com.ipd.kafka.mapping.KafkaMapService;
import com.ipd.jmq.server.store.StoreConfig;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.model.Acknowledge;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.common.network.v3.command.Header;
import com.ipd.jmq.toolkit.lang.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by zhangkepeng on 16-7-29.
 * Modified by luoruiheng
 */
public class ProduceHandler extends AbstractHandler implements KafkaHandler {
    private final static Logger logger = LoggerFactory.getLogger(ProduceHandler.class);

    private KafkaClusterManager kafkaClusterManager;
    private StoreConfig storeConfig;
    private KafkaMapService kafkaMapService;
    private PutMessageHandler putMessageHandler;

    // 主题对应的生产者
    //private ConcurrentMap<String, String> topicApp = new ConcurrentHashMap<String, String>();

    public ProduceHandler(KafkaClusterManager kafkaClusterManager, PutMessageHandler putMessageHandler, BrokerConfig brokerConfig) {
        Preconditions.checkArgument(kafkaClusterManager != null, "KafkaClusterManager can't be null");
        Preconditions.checkArgument(putMessageHandler != null, "PutMessageHandler can't be null");
        this.kafkaClusterManager = kafkaClusterManager;
        this.storeConfig = brokerConfig.getStoreConfig();
        Preconditions.checkArgument(storeConfig != null, "StoreConfig can't be null");
        this.putMessageHandler = putMessageHandler;
        this.kafkaMapService = new KafkaMapService(storeConfig);
    }

    @Override
    public Command process(final Transport transport, final Command command) {
        if (command == null) {
            return null;
        }
        Command response = null;
        final ProduceRequest produceRequest = (ProduceRequest) command.getPayload();
        if (produceRequest == null) {
            logger.warn("produce request is null");
            return null;
        }
        final short requiredAcks = produceRequest.getRequiredAcks();
        final String clientId = produceRequest.getClientId();
        final ProduceResponse produceResponse = new ProduceResponse();
        final Map<String, Map<Integer, ByteBufferMessageSet>> topicPartitionMap = produceRequest.getTopicPartitionMessages();
        final Map<String, List<ProducerPartitionStatus>> producerResponseStatusMap = new HashMap<String, List<ProducerPartitionStatus>>();
        final Set<String> topics = topicPartitionMap.keySet();

        // 添加主题对应的应用
        for (final String topic : topics) {
            // 转换成JMQ命令，生产消息
            final Map<Integer, ByteBufferMessageSet> partitionMessagesMap = topicPartitionMap.get(topic);
            final Set<Integer> partitions = partitionMessagesMap.keySet();
            final List<ProducerPartitionStatus> partitionResponseStatuss = new ArrayList<ProducerPartitionStatus>();

            if (topics.size() == 1) {
                List<String> apps = kafkaClusterManager.getProducerApp(topic);
                /*String app = topicApp.get(topic);
                if (app == null) {
                    if (apps != null && !apps.isEmpty()) {
                        app = apps.get(0);
                        String oldApp = topicApp.putIfAbsent(topic, app);
                        if (oldApp != null) {
                            app = oldApp;
                        }
                    }
                }*/

//                if (null == clientId || clientId.isEmpty()
//                        || (null != apps && !apps.contains(clientId))) {
//                    addPartitionStatus(partitionResponseStatuss, partitions, ErrorCode.TOPIC_AUTHORIZATION_FAILED, -1L);
//                    producerResponseStatusMap.put(topic, partitionResponseStatuss);
//                    continue;
//                }

                try {
                    // 转换成JMQ Command命令
                    PutMessage putMessage = kafkaMapService.map2PutMessage(topic, clientId, partitions, partitionMessagesMap);
                    Command cmd;
                    Header jmqHeader = JMQHeader.Builder.request();
                    if (requiredAcks != 0 && requiredAcks != 1) {
                        jmqHeader.setAcknowledge(Acknowledge.ACK_FLUSH);
                    } else if (requiredAcks == 0) {
                        jmqHeader.setAcknowledge(Acknowledge.ACK_NO);
                    } else {
                        jmqHeader.setAcknowledge(Acknowledge.ACK_RECEIVE);
                    }
                    cmd = new Command(jmqHeader, putMessage);
                    // 重要消息回调获取状态
                    Command.Callback callBack = new Command.Callback() {
                        @Override
                        public void execute(Object object) {
                            PutResult putResult = (PutResult)object;
                            // 生产错误代码映射
                            short status = ErrorCode.codeFor(putResult.getCode().getCode());
                            // 复制错误代码映射
                            if (status == ErrorCode.NO_ERROR) {
                                status = ErrorCode.codeFor(putResult.getReplicationCode().getCode());
                            }

                            // 设置 callback 的 offset
                            Map<Integer, Long> offsetOfPartitions = new HashMap<Integer, Long>();
                            List<MessageLocation> locations = putResult.getLocation();
                            if (null != locations && !locations.isEmpty()) {
                                for (MessageLocation location : locations) {
                                    short queueId = location.getQueueId();
                                    int partition = KafkaMapService.queue2Partition(queueId);
                                    long offsetOfPartition = KafkaMapService.map2Offset(location.getQueueOffset(), KafkaMapService.JMQ_OFFSET_TO_KAFKA_OFFSET);
                                    if (offsetOfPartitions.get(partition) == null) {
                                        offsetOfPartitions.put(partition, offsetOfPartition);
                                    }
                                    //logger.info(queueId + ": " + offsetOfPartition / 22);
                                }
                            }

                            // 将JMQ队列映射为Kafka
                            for (int partition : partitions) {
                                Long offset = offsetOfPartitions.get(partition);
                                offset = offset == null? -1L : offset;
                                ProducerPartitionStatus partitionStatus = new ProducerPartitionStatus(status, offset);
                                partitionStatus.setPartition(partition);
                                partitionStatus.setErrorCode(status);
                                partitionResponseStatuss.add(partitionStatus);
                            }
                            producerResponseStatusMap.put(topic, partitionResponseStatuss);
                            produceResponse.setCorrelationId(produceRequest.getCorrelationId());
                            produceResponse.setProducerResponseStatuss(producerResponseStatusMap);

                            // 构造应答
                            Command response = new Command(KafkaHeader.Builder.response(command.getHeader().getRequestId()),
                                    produceResponse);
                            try {
                                transport.acknowledge(command, response, null);
                            } catch (TransportException e) {
                                logger.warn("send kafka ack for {} failed: ", topic, e);
                            }

                        }
                    };
                    cmd.setObject(callBack);
                    response = putMessageHandler.process(transport, cmd);
                    // 只有需要完成服务的才进行复制
                    if (requiredAcks == 0 || requiredAcks == 1) {
                        short status = ErrorCode.UNKNOWN;
                        if (response != null) {
                            JMQHeader header = (JMQHeader)response.getHeader();
                            status = ErrorCode.codeFor(header.getStatus());
                        }
                        addPartitionStatus(partitionResponseStatuss, partitions, status, -1L);
                    } else {
                        return null;
                    }
                } catch (Throwable e) {
                    logger.error(e.getMessage(), e);
                    short status = ErrorCode.UNKNOWN;
                    if (e instanceof JMQException) {
                        status = ErrorCode.codeFor(((JMQException) e).getCode());
                    } else {
                        status = ErrorCode.codeFor(e.getClass());
                    }
                    addPartitionStatus(partitionResponseStatuss, partitions, status, -1L);
                }
            } else {
                short status = ErrorCode.UNKNOWN;
                addPartitionStatus(partitionResponseStatuss, partitions, status, -1L);
            }

            producerResponseStatusMap.put(topic, partitionResponseStatuss);
        }
        produceResponse.setCorrelationId(produceRequest.getCorrelationId());
        produceResponse.setProducerResponseStatuss(producerResponseStatusMap);
        // ack等于0，无需应答
        if (requiredAcks == 0) {
            // TODO: 3/16/17 虽然无需应答，但有错误应该关闭连接，以便让客户端知道
            response = null;
        } else {
            KafkaHeader kafkaHeader = KafkaHeader.Builder.response(command.getHeader().getRequestId());
            response = new Command(kafkaHeader, produceResponse);
        }

        return response;
    }

    private void addPartitionStatus(List<ProducerPartitionStatus> partitionResponseStatuss, Set<Integer> partitions, short status, long offset) {
        for (int partition : partitions) {
            ProducerPartitionStatus producerPartitionStatus = new ProducerPartitionStatus(status, offset);
            producerPartitionStatus.setPartition(partition);
            partitionResponseStatuss.add(producerPartitionStatus);
        }
    }

    @Override
    public int[] type() {
        return new int[]{KafkaCommandKeys.PRODUCE};
    }
}
