package com.ipd.jmq.common.network.kafka.codec.encode;

import com.ipd.jmq.common.network.kafka.model.KafkaBroker;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.command.TopicMetadataResponse;
import com.ipd.jmq.common.network.kafka.model.PartitionMetadata;
import com.ipd.jmq.common.network.kafka.model.TopicMetadata;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.Set;

/**
 * Created by zhangkepeng on 16-8-2.
 */
public class TopicMetadataEncoder implements PayloadEncoder<KafkaRequestOrResponse> {

    @Override
    public void encode(KafkaRequestOrResponse payload, ByteBuf buf) throws Exception{
        if (payload == null || buf == null) {
            return;
        }
        TopicMetadataResponse response = (TopicMetadataResponse) payload;
        // 记录开始写位置
        int begin = buf.writerIndex();
        // 四字节长度
        buf.writeInt(0);
        buf.writeInt(response.getCorrelationId());
        Set<KafkaBroker> kafkaBrokers = response.getKafkaBrokers();
        buf.writeInt(kafkaBrokers.size());
        for (KafkaBroker kafkaBroker : kafkaBrokers) {
            buf.writeInt(kafkaBroker.getId());
            CommandUtils.writeShortString(buf, kafkaBroker.getHost());
            buf.writeInt(kafkaBroker.getPort());
        }
        List<TopicMetadata> topicMetadatas = response.getTopicMetadatas();
        buf.writeInt(topicMetadatas.size());
        for (TopicMetadata topicMetadata : topicMetadatas) {
            buf.writeShort(topicMetadata.getErrorCode());
            CommandUtils.writeShortString(buf, topicMetadata.getTopic());
            List<PartitionMetadata> partitionMetadatas = topicMetadata.getPartitionMetadatas();
            buf.writeInt(partitionMetadatas.size());
            for (PartitionMetadata partitionMetadata : partitionMetadatas) {
                buf.writeShort(partitionMetadata.getErrorCode());
                buf.writeInt(partitionMetadata.getPartitionId());
                int leader = -1;
                KafkaBroker leaderBroker = partitionMetadata.getLeader();
                if (leaderBroker != null) {
                    leader = leaderBroker.getId();
                }
                buf.writeInt(leader);
                Set<KafkaBroker> replicas = partitionMetadata.getReplicas();
                buf.writeInt(replicas.size());
                for (KafkaBroker replica : replicas) {
                    buf.writeInt(replica.getId());
                }
                Set<KafkaBroker> isrs = partitionMetadata.getIsr();
                buf.writeInt(isrs.size());
                for (KafkaBroker isr : isrs) {
                    buf.writeInt(isr.getId());
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
        return KafkaCommandKeys.METADATA;
    }
}
