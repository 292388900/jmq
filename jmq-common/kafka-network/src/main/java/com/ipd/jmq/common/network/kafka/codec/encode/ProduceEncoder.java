package com.ipd.jmq.common.network.kafka.codec.encode;

import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.command.ProduceResponse;
import com.ipd.jmq.common.network.kafka.model.ProducerPartitionStatus;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhangkepeng on 16-8-2.
 */
public class ProduceEncoder implements PayloadEncoder<KafkaRequestOrResponse> {

    @Override
    public void encode(KafkaRequestOrResponse payload, ByteBuf buf) throws Exception{
        if (payload == null || buf == null) {
            return;
        }
        ProduceResponse response = (ProduceResponse) payload;
        // 记录开始写位置
        int begin = buf.writerIndex();
        // 四字节长度
        buf.writeInt(0);
        buf.writeInt(response.getCorrelationId());
        Map<String, List<ProducerPartitionStatus>> topicResponseStatusMap = response.getProducerResponseStatuss();
        Set<String> topics = topicResponseStatusMap.keySet();
        buf.writeInt(topics.size());
        for (String topic : topics) {
            CommandUtils.writeShortString(buf, topic);
            List<ProducerPartitionStatus> producerPartitionStatuses = topicResponseStatusMap.get(topic);
            buf.writeInt(producerPartitionStatuses.size());
            for (ProducerPartitionStatus partitionStatus : producerPartitionStatuses) {
                buf.writeInt(partitionStatus.getPartition());
                buf.writeShort(partitionStatus.getErrorCode());
                buf.writeLong(partitionStatus.getOffset());
            }
        }
        buf.writeInt(0);
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
        return KafkaCommandKeys.PRODUCE;
    }
}
