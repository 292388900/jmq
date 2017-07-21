package com.ipd.kafka;

import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.network.v3.command.GetMessage;
import com.ipd.jmq.common.network.v3.command.GetMessageAck;
import com.ipd.jmq.common.network.v3.command.JMQHeader;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.kafka.command.FetchRequest;
import com.ipd.jmq.common.network.kafka.command.FetchResponse;
import com.ipd.jmq.common.network.kafka.command.KafkaHeader;
import com.ipd.jmq.common.network.kafka.exception.ErrorCode;
import com.ipd.jmq.common.network.kafka.message.ByteBufferMessageSet;
import com.ipd.jmq.common.network.kafka.message.KafkaMessage;
import com.ipd.jmq.common.network.kafka.model.CompressionCodec;
import com.ipd.jmq.common.network.kafka.model.DelayedResponseKey;
import com.ipd.jmq.common.network.kafka.model.FetchResponsePartitionData;
import com.ipd.jmq.common.network.kafka.model.TopicAndPartition;
import com.ipd.jmq.common.network.v3.session.Consumer;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.handler.GetMessageHandler;
import com.ipd.jmq.server.store.ConsumeQueue;
import com.ipd.kafka.mapping.KafkaMapService;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.toolkit.buffer.RByteBuffer;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.service.ServiceThread;
import com.ipd.jmq.toolkit.time.SystemClock;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by zhangkepeng on 16-12-5.
 */
public class DelayedResponseManager extends Service{

    private static final Logger logger = LoggerFactory.getLogger(DelayedResponseManager.class);

    // 长轮询线程池
    protected ExecutorService executorService;
    // kafka配置
    protected BrokerConfig brokerConfig;

    // Kafka长轮询请求Queue
    protected Queue<Object> kafkaLongPulls = new LinkedBlockingDeque<Object>();
    // Kafka异步处理
    protected Thread kafkaGuardThread = null;
    // 消费处理
    protected GetMessageHandler getMessageHandler;

    public DelayedResponseManager(GetMessageHandler getMessageHandler, ExecutorService executorService, BrokerConfig brokerConfig) {
        Preconditions.checkArgument(executorService != null, "ExecutorService can not be null");
        Preconditions.checkArgument(brokerConfig != null, "BrokerConfig can not be null");
        this.executorService = executorService;
        this.brokerConfig = brokerConfig;
        this.getMessageHandler = getMessageHandler;
        Preconditions.checkArgument(getMessageHandler != null, "GetMessageHandler can not be null");
    }

    @Override
    protected void doStart() throws Exception {
        // kafka异步处理
        kafkaGuardThread = new Thread(new ServiceThread(this, 100) {
            @Override
            public boolean onException(Throwable e) {
                logger.error(e.getMessage(), e);
                return true;
            }
            @Override
            protected void execute() throws Exception {
                // 处理kafka长轮询
                delayedProcess();
            }
        }, "kafka LongPull");
        kafkaGuardThread.start();
    }


    /**
     * kafka长轮询
     * @return
     */
    public boolean suspend(Object object) {

        // 超过最大容量
        int maxLongPulls = brokerConfig.getMaxLongPulls();
        if (kafkaLongPulls.size() > maxLongPulls) {
            return false;
        }

        synchronized (kafkaLongPulls) {
            // 入队
            // TODO: 3/17/17 过期时间内一直入队
            if (kafkaLongPulls.offer(object)) {
                return true;
            }
        }
        return false;
    }

    public void closeTransportAndClearQueue(Transport transport) {
        synchronized (kafkaLongPulls) {
            for (Object element : kafkaLongPulls.toArray()) {
                if (element instanceof DelayedResponseKey) {
                    DelayedResponseKey delayedResponse = (DelayedResponseKey)element;
                    Transport delayedResponseTransport = delayedResponse.getTransport();
                    delayedResponseTransport.stop();
                }
            }
            transport.stop();
            kafkaLongPulls.clear();
            logger.warn("send delayed response failed: ");
        }
    }

    /**
     * Kafka处理长轮询请求，检查是否过期，是否有数据了
     */
    protected void delayedProcess() throws Exception {
        int size = kafkaLongPulls.size();
        for (int i = 0; i < size; i++) {
            if (!isStarted()) {
                return;
            }
            synchronized (kafkaLongPulls) {
                Object object = kafkaLongPulls.poll();
                if (object instanceof DelayedResponseKey) {
                    DelayedResponseKey delayedResponseKey = (DelayedResponseKey) object;
                    if (delayedResponseKey.getType() == DelayedResponseKey.Type.FETCH) {
                        fetchForLongPull(delayedResponseKey);
                    } else {
                        Transport transport = delayedResponseKey.getTransport();
                        try {
                            transport.acknowledge(delayedResponseKey.getRequest(), delayedResponseKey.getResponse(), null);
                        } catch (TransportException e) {
                            closeTransportAndClearQueue(transport);
                        }
                    }
                }
            }
        }
    }

    public void fetchForLongPull(DelayedResponseKey delayedResponseKey) throws TransportException {
        long currentTime = SystemClock.now();
        if (delayedResponseKey.getExpire() >= currentTime) {
            acknowledge(delayedResponseKey);
        } else {
            fetchMessage(delayedResponseKey);
        }
    }

    private void acknowledge(DelayedResponseKey delayedResponseKey) {
        Command request = delayedResponseKey.getRequest();
        FetchRequest fetchRequest = (FetchRequest)request.getPayload();
        FetchResponse fetchResponse = new FetchResponse();
        fetchResponse.setCorrelationId(fetchRequest.getCorrelationId());

        Map<String, Map<Integer, FetchResponsePartitionData>> fetchResponseMap = new HashMap<String, Map<Integer, FetchResponsePartitionData>>();
        Map<String, Map<Integer, FetchRequest.PartitionFetchInfo>> fetchInfoMap = fetchRequest.getRequestInfo();
        Set<String> topics = fetchInfoMap.keySet();
        for (String topic : topics) {
            Map<Integer, FetchRequest.PartitionFetchInfo> partitionInfoMap = fetchInfoMap.get(topic);
            Set<Integer> partitions = partitionInfoMap.keySet();
            Map<Integer, FetchResponsePartitionData> partitionResponseMap = new HashMap<Integer, FetchResponsePartitionData>();
            for (int partition : partitions) {
                FetchRequest.PartitionFetchInfo partitionFetchInfo = partitionInfoMap.get(partition);
                long kafkaOffset = partitionFetchInfo.getOffset();
                FetchResponsePartitionData fetchResponsePartitionData = new FetchResponsePartitionData(ErrorCode.NO_ERROR, kafkaOffset, ByteBufferMessageSet.emptySet);
                partitionResponseMap.put(partition, fetchResponsePartitionData);
            }
            fetchResponseMap.put(topic, partitionResponseMap);
        }
        fetchResponse.setCorrelationId(fetchRequest.getCorrelationId());
        // 0.8 不能设置 ThrottleTimeMs
        if (fetchRequest.getVersion() > 0) {
            fetchResponse.setThrottleTimeMs(0);
        }
        fetchResponse.setFetchResponses(fetchResponseMap);
        Command response = new Command(KafkaHeader.Builder.response(request.getHeader().getRequestId()), fetchResponse);
        // 长轮询过期了
        Transport transport = delayedResponseKey.getTransport();
        try {
            transport.acknowledge(request, response, null);
        } catch (TransportException e) {
            closeTransportAndClearQueue(transport);
        }
    }

    private void fetchMessage(DelayedResponseKey delayedResponseKey) {

        if (delayedResponseKey == null) {
            return;
        }
        Transport transport = delayedResponseKey.getTransport();
        Command request = delayedResponseKey.getRequest();
        FetchRequest fetchRequest = (FetchRequest)request.getPayload();
        Map<String, Map<Integer, FetchRequest.PartitionFetchInfo>> fetchInfoMap = fetchRequest.getRequestInfo();
        Set<TopicAndPartition> topicAndPartitions = new HashSet<TopicAndPartition>();
        Set<String> topics = fetchInfoMap.keySet();
        // 读取的消息字节
        int bytesReadable = 0;
        // 是否报错
        boolean errorReadingData = false;
        // 主题——》partition-》对应获取消息
        Map<String, Map<Integer, FetchResponsePartitionData>> fetchResponseMap = new HashMap<String, Map<Integer, FetchResponsePartitionData>>();
        for (String topic : topics) {
            Map<Integer, FetchRequest.PartitionFetchInfo> partitionInfoMap = fetchInfoMap.get(topic);
            Set<Integer> partitions = partitionInfoMap.keySet();
            Map<Integer, FetchResponsePartitionData> partitionResponseMap = new HashMap<Integer, FetchResponsePartitionData>();
            for (int partition : partitions) {
                topicAndPartitions.add(new TopicAndPartition(topic, partition));
                FetchRequest.PartitionFetchInfo partitionFetchInfo = partitionInfoMap.get(partition);
                long kafkaOffset = partitionFetchInfo.getOffset();
                int fetchSize = partitionFetchInfo.getFetchSize();
                try {
                    // 读取消息体
                    FetchResponsePartitionData fetchDataInfo = readMessageSet(transport, topic, partition, kafkaOffset, fetchSize);
                    if (fetchDataInfo.getError() != ErrorCode.NO_ERROR) {
                        errorReadingData = true;
                    }
                    bytesReadable += fetchDataInfo.getByteBufferMessageSet().sizeInBytes();
                    partitionResponseMap.put(partition, fetchDataInfo);
                } catch (Throwable e) {
                    short status = ErrorCode.UNKNOWN;
                    if (e instanceof JMQException) {
                        status = ErrorCode.codeFor(((JMQException) e).getCode());
                        logger.error(String.format("Error when processing fetch request for partition [%s,%d] offset %d with correlation id %d. Possible cause: %s",
                                topic, partition, kafkaOffset, fetchRequest.getCorrelationId(), e.getMessage()));
                        partitionResponseMap.put(partition, new FetchResponsePartitionData(status, kafkaOffset, ByteBufferMessageSet.emptySet));
                    } else {
                        status = ErrorCode.codeFor(e.getClass());
                        logger.error(String.format("Error when processing fetch request for partition [%s,%d] offset %d with correlation id %d. Possible cause: %s",
                                topic, partition, kafkaOffset, fetchRequest.getCorrelationId(), e.getMessage()));
                        partitionResponseMap.put(partition, new FetchResponsePartitionData(status, kafkaOffset, ByteBufferMessageSet.emptySet));
                    }
                }
            }
            fetchResponseMap.put(topic, partitionResponseMap);
        }
        FetchResponse fetchResponse = new FetchResponse();
        fetchResponse.setCorrelationId(fetchRequest.getCorrelationId());
        // 0.8 不能设置 ThrottleTimeMs
        if (fetchRequest.getVersion() > 0) {
            fetchResponse.setThrottleTimeMs(0);
        }
        fetchResponse.setFetchResponses(fetchResponseMap);
        // send the data immediately if 1) fetch request does not want to wait
        //                              2) fetch request does not require any data
        //                              3) has enough data to respond
        //                              4) some error happens while reading data
        if (fetchRequest.getMaxWait() <= 0 ||
                fetchRequest.getNumPartitions() <= 0 ||
                bytesReadable >= fetchRequest.getMinBytes() ||
                errorReadingData) {
            Command response = new Command(KafkaHeader.Builder.response(request.getHeader().getRequestId()), fetchResponse);
            try {
                transport.acknowledge(request, response, null);
            } catch (TransportException e) {
                closeTransportAndClearQueue(transport);
            }
        } else if (isStarted()) {
            acknowledge(delayedResponseKey);
        }
    }

    private FetchResponsePartitionData readMessageSet(Transport transport, String topic, int partition, long kafkaOffset, int fetchSize) throws Exception{
        FetchResponsePartitionData fetchResponsePartitionData = null;
        short error = ErrorCode.NO_ERROR;
        // Kafka偏移量映射为JMQ偏移量
        long offset = KafkaMapService.map2Offset(kafkaOffset, KafkaMapService.KAFKA_OFFSET_TO_JMQ_OFFSET);
        // partition映射为队列号
        short queue = KafkaMapService.partition2Queue(partition);
        ByteBuffer byteBuffer = ByteBuffer.allocate(fetchSize);
        while (fetchSize > 0) {
            RByteBuffer[] buffers = null;
            try {
                Command request = createJMQRequest(topic, queue, offset);
                Command response = getMessageHandler.process(transport, request);
                GetMessageAck getMessageAck = null;
                if (response != null) {
                    // JMQException异常，获取异常码
                    JMQHeader jmqHeader = (JMQHeader)response.getHeader();
                    int status = jmqHeader.getStatus();
                    if (status != JMQCode.SUCCESS.getCode()) {
                        error = ErrorCode.codeFor(status);
                        break;
                    }
                    getMessageAck = (GetMessageAck) response.getPayload();
                }
                buffers = getMessageAck.getBuffers();
                if (getMessageAck == null || buffers == null || buffers.length == 0) {
                    if (byteBuffer.position() != 0) {
                        fetchResponsePartitionData = getFetchResponsePartitionData(byteBuffer, offset, error);
                    } else {
                        fetchResponsePartitionData = new FetchResponsePartitionData(error, kafkaOffset, ByteBufferMessageSet.emptySet);
                    }
                    return fetchResponsePartitionData;
                }
                for (RByteBuffer rBuffer : buffers) {
                    // 获取到消息后JMQ偏移量自动更新
                    BrokerMessage message = Serializer.readBrokerMessage(Unpooled.wrappedBuffer(rBuffer.getBuffer()));
                    offset = message.getQueueOffset() + ConsumeQueue.CQ_RECORD_SIZE;
                    int messageSize;
                    byte[] byteBody = message.getByteBody();
                    if (message.getSource() == BrokerMessage.MESSAGE_FROM_KAFKA) {
                        ByteBuffer buffer = ByteBuffer.wrap(byteBody);
                        messageSize = buffer.limit();
                        if (fetchSize > messageSize) {
                            long kafkaOffsetValue = KafkaMapService.map2Offset(message.getQueueOffset(), KafkaMapService.JMQ_OFFSET_TO_KAFKA_OFFSET);
                            setQueueOffset(buffer, kafkaOffsetValue);
                            byteBuffer.put(buffer);
                        }
                    } else {
                        KafkaMessage kafkaMessage = new KafkaMessage(byteBody, null, CompressionCodec.NoCompressionCodec);
                        ByteBuffer kafkaByteBuffer = ByteBuffer.allocate(kafkaMessage.size() + ByteBufferMessageSet.LOG_OVER_HEAD);
                        // 需要将JMQ偏移量映射为Kafka偏移量
                        long kafkaOffsetValue = KafkaMapService.map2Offset(message.getQueueOffset(), KafkaMapService.JMQ_OFFSET_TO_KAFKA_OFFSET);
                        ByteBufferMessageSet.writeMessage(kafkaByteBuffer, kafkaMessage, kafkaOffsetValue);
                        messageSize = ByteBufferMessageSet.entrySize(kafkaMessage);
                        kafkaByteBuffer.rewind();
                        if (fetchSize > messageSize) {
                            byteBuffer.put(kafkaByteBuffer);
                        }
                    }
                    fetchSize -= messageSize;
                    if (fetchSize - messageSize < 0) {
                        break;
                    }
                }
            } catch (Throwable e) {
                throw e;
            } finally {
                if (buffers != null) {
                    for (RByteBuffer buffer : buffers) {
                        buffer.release();
                        buffer.clear();
                    }
                }
                buffers = null;
            }
        }
        fetchResponsePartitionData = getFetchResponsePartitionData(byteBuffer, offset, error);
        return fetchResponsePartitionData;
    }

    private FetchResponsePartitionData getFetchResponsePartitionData(ByteBuffer byteBuffer, long offset, short errorCode) {
        long hw;
        FetchResponsePartitionData fetchResponsePartitionData;
        int position = byteBuffer.position();
        byteBuffer.rewind();
        ByteBuffer messageBuffer = byteBuffer.slice();
        messageBuffer.limit(position);
        // 构造kafka消息集
        ByteBufferMessageSet byteBufferMessageSet = new ByteBufferMessageSet(messageBuffer);
        // JMQ偏移量转换成Kafka偏移量
        hw = KafkaMapService.map2Offset(offset, KafkaMapService.JMQ_OFFSET_TO_KAFKA_OFFSET);
        fetchResponsePartitionData = new FetchResponsePartitionData(errorCode, hw, byteBufferMessageSet);
        return fetchResponsePartitionData;
    }

    private Command createJMQRequest(String topic, short queue, long offset) {
        GetMessage getMessage = new GetMessage();
        getMessage.setLongPull(0);
        getMessage.setTopic(topic);
        getMessage.setQueueId(queue);
        getMessage.setOffset(offset);
        Command request = new Command();
        request.setHeader(JMQHeader.Builder.request());
        request.setPayload(getMessage);
        request.setObject(Consumer.ConsumeType.KAFKA);
        return request;
    }

    private void setQueueOffset(ByteBuffer buffer, long queueOffset) {
        buffer.mark();
        int position = 0;
        buffer.position(position);
        buffer.putLong(queueOffset);
        buffer.reset();
    }
}
