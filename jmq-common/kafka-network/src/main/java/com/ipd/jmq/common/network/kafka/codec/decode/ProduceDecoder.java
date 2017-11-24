package com.ipd.jmq.common.network.kafka.codec.decode;

import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.command.ProduceRequest;
import com.ipd.jmq.common.network.kafka.message.ByteBufferMessageSet;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangkepeng on 16-8-2.
 */
public class ProduceDecoder implements PayloadDecoder<KafkaRequestOrResponse> {

    @Override
    public KafkaRequestOrResponse decode(KafkaRequestOrResponse payload, ByteBuf in) throws Exception{
        if (in == null) {
            return null;
        }
        ProduceRequest produceRequest = (ProduceRequest) payload;
        produceRequest.setVersion(in.readShort());
        produceRequest.setCorrelationId(in.readInt());
        produceRequest.setClientId(CommandUtils.readShortString(in));
        // 2字节应答类型
        produceRequest.setRequiredAcks(in.readShort());
        // 4字节等待应答超时
        produceRequest.setAckTimeoutMs(in.readInt());
        // 4字节主题个数
        int topicCount = in.readInt();
        Map<String, Map<Integer, ByteBufferMessageSet>> topicPartitionMessages = new HashMap<String, Map<Integer, ByteBufferMessageSet>>();
        for (int i = 0; i < topicCount; i++) {
            // 主题
            String topic = CommandUtils.readShortString(in);
            // partition数量
            int partitionCount = in.readInt();
            Map<Integer, ByteBufferMessageSet> partitionMessages = new HashMap<Integer, ByteBufferMessageSet>();
            for (int j = 0; j < partitionCount; j++) {
                int partition = in.readInt();
                int messageSetSize = in.readInt();
                byte[] bytes = new byte[messageSetSize];
                in.readBytes(bytes, 0, messageSetSize);
                ByteBufferMessageSet byteBufferMessageSet = new ByteBufferMessageSet(ByteBuffer.wrap(bytes));
                partitionMessages.put(partition, byteBufferMessageSet);
            }
            topicPartitionMessages.put(topic, partitionMessages);
        }
        produceRequest.setTopicPartitionMessages(topicPartitionMessages);
        return produceRequest;
    }

    @Override
    public short type() {
        return KafkaCommandKeys.PRODUCE;
    }


}
