package com.ipd.kafka.mapping;

import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.network.kafka.exception.KafkaException;
import com.ipd.jmq.common.network.kafka.exception.MessageSizeTooLargeException;
import com.ipd.jmq.common.network.kafka.message.ByteBufferMessageSet;
import com.ipd.jmq.common.network.kafka.message.KafkaMessageAndOffset;
import com.ipd.jmq.common.network.kafka.model.CompressionCodec;
import com.ipd.jmq.common.network.kafka.model.KafkaLogAppendInfo;
import com.ipd.jmq.common.network.v3.command.PutMessage;
import com.ipd.jmq.server.store.ConsumeQueue;
import com.ipd.jmq.server.store.StoreConfig;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by zhangkepeng on 16-12-6.
 * 目前仅支持扩容，不支持缩容和迁移
 */
@NotThreadSafe
public class KafkaMapService {

    public final static Logger logger = LoggerFactory.getLogger(KafkaMapService.class);

    public static final short KAFKA_OFFSET_TO_JMQ_OFFSET = 1;
    public static final short JMQ_OFFSET_TO_KAFKA_OFFSET = 2;

    // 队列与partition映射关系
    private static AbstractQueueMap<Integer> queueMap;

    // 存储配置
    private StoreConfig storeConfig;

    // kafka消息集处理
    private ByteBufferSetHandler byteBufferSetHandler;

    public KafkaMapService() {

    }

    public KafkaMapService(StoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        Preconditions.checkArgument(storeConfig != null, "StoreConfig can't be null");
        this.byteBufferSetHandler = new ByteBufferSetHandler(storeConfig);
        this.queueMap = new DefaultQueueMap();

    }

    public PutMessage map2PutMessage(String topic, String app, Set<Integer> partitions, Map<Integer, ByteBufferMessageSet> partitionMessagesMap) throws Exception{
        /**
         * 如果是异步发送，partitions可能是多个
         * 原有的逻辑有bug(PutMessage只会是最后一个partition对应的message)
         * 其他partitions的数据会丢失
         */
        PutMessage putMessage = new PutMessage();
        List<BrokerMessage> allMessages = new ArrayList<BrokerMessage>();

        for (int partition : partitions) {
            ByteBufferMessageSet byteBufferMessageSet = partitionMessagesMap.get(partition);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("put message:topic is %s and partition is %d", topic, partition));
            }

            List<BrokerMessage> brokerMessages = map2BrokerMessages(topic, app, partition, byteBufferMessageSet);

            if (brokerMessages != null && !brokerMessages.isEmpty()) {
                allMessages.addAll(brokerMessages);
            }
        }
        if (!allMessages.isEmpty()) {
            Message[] messages = allMessages.toArray(new Message[allMessages.size()]);
            putMessage.setMessages(messages);
            putMessage.setQueueId((short)0);
        }
        return putMessage;
    }

    /**
     * Kafka消息转换成JMQ消息
     */
    public List<BrokerMessage> map2BrokerMessages(String topic, String app, int partition, ByteBufferMessageSet byteBufferMessageSet) throws Exception {

        List<BrokerMessage> mappedBrokerMessages = new ArrayList<BrokerMessage>();
        // partition映射成队列
        short queue = partition2Queue(partition);
        KafkaLogAppendInfo kafkaLogAppendInfo = byteBufferSetHandler.analyzeAndValidateMessageSet(byteBufferMessageSet);
        if (kafkaLogAppendInfo.getShallowCount() == 0) {
            return mappedBrokerMessages;
        }
        // trim any invalid bytes or partial messages before appending it to the on-disk log
        ByteBufferMessageSet validMessages = byteBufferSetHandler.trimInvalidBytes(byteBufferMessageSet, kafkaLogAppendInfo);
        // 转换消息
        CompressionCodec codec = kafkaLogAppendInfo.getCodec();
        Iterator<KafkaMessageAndOffset> kafkaMessageAndOffsets = byteBufferSetHandler.getALLKafakMessageAndOffsets(validMessages, codec);
        while (kafkaMessageAndOffsets != null && kafkaMessageAndOffsets.hasNext()) {
            KafkaMessageAndOffset kafkaMessageAndOffset = kafkaMessageAndOffsets.next();
            // 判断解压后消息是否大于最大消息体
            if (ByteBufferMessageSet.entrySize(kafkaMessageAndOffset.getMessage()) > storeConfig.getMaxMessageSize()) {
                throw new MessageSizeTooLargeException(String.format("Message size is %d bytes which exceeds the maximum configured message size of %d."
                        , ByteBufferMessageSet.entrySize(kafkaMessageAndOffset.getMessage()), storeConfig.getMaxMessageSize()));
            }
            // 构造解压锁后消息
            ByteBuffer byteBuffer = kafkaMessageAndOffset.getMessage().buffer;
            ByteBuffer storeMessageBuffer = ByteBuffer.allocate(byteBuffer.limit() + ByteBufferMessageSet.LOG_OVER_HEAD);
            ByteBufferMessageSet.writeMessage(storeMessageBuffer, kafkaMessageAndOffset.getMessage(), 0L);
            storeMessageBuffer.rewind();
            // 消息已被解压
            BrokerMessage brokerMessage = getBrokerMessage(topic, app, queue, storeMessageBuffer);
            mappedBrokerMessages.add(brokerMessage);
        }
        return mappedBrokerMessages;
    }


    public BrokerMessage getBrokerMessage(String topic, String app, short queue, ByteBuffer byteBuffer) {
        ByteBuffer messageBody = byteBuffer;
        int kafkaMessageSize = byteBuffer.limit();

        // 构造BrokerMessage对象
        BrokerMessage message = new BrokerMessage();
        short code = 0;
        // 没有压缩
        message.setCompressed(false);
        // 非顺序
        message.setOrdered(false);
        message.setFlag(code);
        // 优先级
        message.setPriority((byte) code);
        // 发送时间
        message.setSendTime(SystemClock.now());

        if (kafkaMessageSize > 0) {
            // 消息体
            message.setBody(messageBody);
        }
        // 设置消息体CRC
        message.setBodyCRC(message.getBodyCRC());
        // 主题
        message.setTopic(topic);
        // 应用
        message.setApp(app);
        // 消息来源设置为kafka
        message.setSource(BrokerMessage.MESSAGE_FROM_KAFKA);
        // 队列
        message.setQueueId(queue);
        return message;
    }

    /**
     * 偏移量映射
     */
    public static long map2Offset(long offset, short type) {

        long mappingOffset = -1L;
        if (type == KAFKA_OFFSET_TO_JMQ_OFFSET) {
            if (offset < 0) {
                throw new KafkaException("invalid kafka offset");
            }
            mappingOffset = offset * ConsumeQueue.CQ_RECORD_SIZE;
        } else if (type == JMQ_OFFSET_TO_KAFKA_OFFSET) {
            if ((offset % ConsumeQueue.CQ_RECORD_SIZE) != 0) {
                throw new KafkaException("invalid jmq offset");
            }
            mappingOffset = offset / ConsumeQueue.CQ_RECORD_SIZE;
        }
        return mappingOffset;
    }

    /**
     * partition映射为队列
     */
    public static short partition2Queue(int partition) {
        if (partition < 0) {
            throw new KafkaException("invalid partition");
        }
        short queue = queueMap.PARTITION2QUEUE.get(partition);
        if (queue < 0 || queue > Short.MAX_VALUE) {
            throw new KafkaException("invalid queue");
        }
        return queue;
    }

    /**
     * 队列映射为partition
     */
    public static int queue2Partition(short queue) {
        if (queue < 1 || queue > Short.MAX_VALUE) {
            throw new KafkaException("invalid queue");
        }
        int partition = queueMap.QUEUE2PARTITION.get(queue);
        if (partition < 0) {
            throw new KafkaException("invalid partition");
        }
        return partition;
    }

    /**
     * 主题分片变化是更新映射缓存
     */
    public static void updatePartition2Queue(Set<Integer> partitions) {
        queueMap.queueMap(partitions);
    }
}
