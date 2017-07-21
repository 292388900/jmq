package com.ipd.jmq.common.network.kafka.codec.encode;

import com.ipd.jmq.common.network.kafka.command.FetchResponse;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.model.FetchResponsePartitionData;
import com.ipd.jmq.common.network.kafka.model.TopicAndPartition;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhangkepeng on 16-8-2.
 */
public class FetchEncoder implements PayloadEncoder<KafkaRequestOrResponse> {

    @Override
    public void encode(KafkaRequestOrResponse payload, ByteBuf buf) throws Exception {
        if (payload == null || buf == null) {
            return;
        }

        FetchResponse response = (FetchResponse) payload;

        // 开始写位置
        int begin = buf.writerIndex();
        // 长度
        buf.writeInt(0);
        // 响应id
        buf.writeInt(response.getCorrelationId());
        if (response.getThrottleTimeMs() >= 0) {
            buf.writeInt(response.getThrottleTimeMs());
        }
        Map<String, Map<Integer, FetchResponsePartitionData>> fetchDataMap = response.getFetchResponses();
        Set<String> topics = fetchDataMap.keySet();
        TopicAndPartition topicAndPartition = new TopicAndPartition();
        buf.writeInt(topics.size());
        for (String topic : topics) {
            CommandUtils.writeShortString(buf, topic);
            Map<Integer, FetchResponsePartitionData> partitionDataMap = fetchDataMap.get(topic);
            Set<Integer> partitions = partitionDataMap.keySet();
            // partition数
            buf.writeInt(partitions.size());
            for (int partition : partitions) {
                topicAndPartition.setPartition(partition);
                topicAndPartition.setTopic(topic);
                FetchResponsePartitionData fetchResponsePartitionData = partitionDataMap.get(partition);
                buf.writeInt(partition);

                buf.writeShort(fetchResponsePartitionData.getError());
                buf.writeLong(fetchResponsePartitionData.getHw());

                ByteBuffer buffer = fetchResponsePartitionData.getByteBufferMessageSet().getBuffer();
                buf.writeInt(buffer.limit());
                byte[] bytes = new byte[buffer.limit()];
                buffer.rewind();
                buffer.get(bytes, 0, buffer.limit());
                buf.writeBytes(bytes);
            }
        }

        // 写结束位置
        int end = buf.writerIndex();
        // 字节长度
        int length = (end - begin -4);
        // 移到开始位置
        buf.writerIndex(begin);
        // 命令长度
        buf.writeInt(length);
        // 恢复写结束位置
        buf.writerIndex(end);
    }

    @Override
    public short type() {
        return KafkaCommandKeys.FETCH;
    }
}
