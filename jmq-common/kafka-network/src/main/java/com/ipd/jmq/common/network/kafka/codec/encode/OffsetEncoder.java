package com.ipd.jmq.common.network.kafka.codec.encode;

import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.command.OffsetResponse;
import com.ipd.jmq.common.network.kafka.model.PartitionOffsetsResponse;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import io.netty.buffer.ByteBuf;

import java.util.Map;
import java.util.Set;

/**
 * Created by zhangkepeng on 16-8-4.
 */
public class OffsetEncoder implements PayloadEncoder<KafkaRequestOrResponse> {

    @Override
    public void encode(KafkaRequestOrResponse payload, ByteBuf buf) throws Exception {
        if (payload == null || buf == null) {
            return;
        }

        OffsetResponse response = (OffsetResponse) payload;
        // 开始写位置
        int begin = buf.writerIndex();
        // 长度
        buf.writeInt(0);
        // 响应id
        buf.writeInt(response.getCorrelationId());
        // 响应的map
        Map<String, Map<Integer, PartitionOffsetsResponse>> topicPartitionOffsets = response.getOffsetsResponseMap();
        Set<String> topics = topicPartitionOffsets.keySet();
        // 主题大小
        buf.writeInt(topics.size());
        for (String topic : topics) {
            // 主题
            CommandUtils.writeShortString(buf, topic);
            Map<Integer, PartitionOffsetsResponse> partitionOffsetsResponseMap = topicPartitionOffsets.get(topic);
            Set<Integer> partitions = partitionOffsetsResponseMap.keySet();
            // partition数量
            buf.writeInt(partitions.size());
            for (int partition : partitions) {
                PartitionOffsetsResponse partitionOffsetsResponse = partitionOffsetsResponseMap.get(partition);
                if (partitionOffsetsResponse != null) {
                    // partition编号
                    buf.writeInt(partition);
                    // 错误码
                    buf.writeShort(partitionOffsetsResponse.getErrorCode());
                    Set<Long> offsets = partitionOffsetsResponse.getOffsets();
                    // 符合条件的偏移量个数
                    buf.writeInt(offsets.size());
                    // 写入偏移量
                    for (long offset : offsets) {
                        buf.writeLong(offset);
                    }
                }
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
        return KafkaCommandKeys.LIST_OFFSETS;
    }
}
