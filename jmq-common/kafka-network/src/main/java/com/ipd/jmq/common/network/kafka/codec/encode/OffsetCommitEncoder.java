package com.ipd.jmq.common.network.kafka.codec.encode;

import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.command.OffsetCommitResponse;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import io.netty.buffer.ByteBuf;

import java.util.Map;
import java.util.Set;

/**
 * Created by zhangkepeng on 16-8-4.
 */
public class OffsetCommitEncoder implements PayloadEncoder<KafkaRequestOrResponse> {

    @Override
    public void encode(KafkaRequestOrResponse payload, ByteBuf buf) throws Exception {
        if (payload == null || buf == null) {
            return;
        }

        OffsetCommitResponse response = (OffsetCommitResponse) payload;
        // 开始写位置
        int begin = buf.writerIndex();
        // 4字节长度
        buf.writeInt(0);
        // 4字节响应id
        buf.writeInt(response.getCorrelationId());

        Map<String, Map<Integer, Short>> commitOffsetStatusMap = response.getCommitStatus();
        Set<String> topics = commitOffsetStatusMap.keySet();
        buf.writeInt(topics.size());
        for (String topic : topics) {
            CommandUtils.writeShortString(buf, topic);
            Map<Integer, Short> partitionStatusMap = commitOffsetStatusMap.get(topic);
            Set<Integer> partitions = partitionStatusMap.keySet();
            buf.writeInt(partitions.size());
            for (int partition : partitions) {
                buf.writeInt(partition);
                short status = partitionStatusMap.get(partition);
                buf.writeShort(status);
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
        return KafkaCommandKeys.OFFSET_COMMIT;
    }
}
