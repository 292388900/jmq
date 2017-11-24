package com.ipd.jmq.common.network.kafka.codec.decode;

import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.command.OffsetCommitRequest;
import com.ipd.jmq.common.network.kafka.model.OffsetAndMetadata;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import com.ipd.jmq.common.network.TransportException;
import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangkepeng on 16-8-2.
 */
public class OffsetCommitDecoder implements PayloadDecoder<KafkaRequestOrResponse> {

    @Override
    public KafkaRequestOrResponse decode(KafkaRequestOrResponse payload, ByteBuf in) throws TransportException.CodecException{
        if (in == null) {
            return null;
        }
        OffsetCommitRequest offsetCommitRequest = (OffsetCommitRequest) payload;
        short version = in.readShort();
        offsetCommitRequest.setVersion(version);
        offsetCommitRequest.setCorrelationId(in.readInt());
        offsetCommitRequest.setClientId(CommandUtils.readShortString(in));
        offsetCommitRequest.setGroupId(CommandUtils.readShortString(in));
        // 0.8 above
        if (version >= 1) {
            offsetCommitRequest.setGroupGenerationId(in.readInt());
            offsetCommitRequest.setMemberId(CommandUtils.readShortString(in));
        }
        // 0.9 above
        if (version >= 2) {
            offsetCommitRequest.setRetentionTime(in.readLong());
        }
        Map<String, Map<Integer, OffsetAndMetadata>> topicPartitionOffsetMetadataMap = new HashMap<String, Map<Integer, OffsetAndMetadata>>();
        int topicCount = in.readInt();
        for (int i = 0; i < topicCount; i++) {
            String topic = CommandUtils.readShortString(in);
            int partitionCount = in.readInt();
            Map<Integer, OffsetAndMetadata> partitionOffsetMetadataMap = new HashMap<Integer, OffsetAndMetadata>();
            for (int j = 0; j < partitionCount; j++) {
                int partitionId = in.readInt();
                long offset = in.readLong();
                long timeStamp = version == 1 ? in.readLong() : offsetCommitRequest.getRetentionTime();
                String metadata = CommandUtils.readShortString(in);
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset, metadata);
                offsetAndMetadata.setOffsetCacheRetainTime(timeStamp);
                partitionOffsetMetadataMap.put(partitionId, offsetAndMetadata);
            }
            topicPartitionOffsetMetadataMap.put(topic, partitionOffsetMetadataMap);
        }
        offsetCommitRequest.setOffsetAndMetadataMap(topicPartitionOffsetMetadataMap);
        return offsetCommitRequest;
    }

    @Override
    public short type() {
        return KafkaCommandKeys.OFFSET_COMMIT;
    }
}
