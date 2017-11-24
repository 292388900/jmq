package com.ipd.jmq.common.network.kafka.codec.decode;

import com.google.common.collect.HashMultimap;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.command.OffsetFetchRequest;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import io.netty.buffer.ByteBuf;

/**
 * Created by zhangkepeng on 16-8-2.
 */
public class OffsetFetchDecoder implements PayloadDecoder<KafkaRequestOrResponse> {

    @Override
    public KafkaRequestOrResponse decode(KafkaRequestOrResponse payload, ByteBuf in) throws Exception{
        HashMultimap<String, Integer> topicAndPartitions = HashMultimap.create();

        OffsetFetchRequest offsetFetchRequest = (OffsetFetchRequest) payload;
        offsetFetchRequest.setVersion(in.readShort());
        offsetFetchRequest.setCorrelationId(in.readInt());
        offsetFetchRequest.setClientId(CommandUtils.readShortString(in));
        offsetFetchRequest.setGroupId(CommandUtils.readShortString(in));
        int topicCount = in.readInt();
        for (int i = 0; i < topicCount; i++) {
            String topic = CommandUtils.readShortString(in);
            int partitionCount = in.readInt();
            for (int j = 0; j < partitionCount; j++) {
                int partitionId = in.readInt();
                topicAndPartitions.put(topic, partitionId);
            }
        }

        offsetFetchRequest.setTopicAndPartitions(topicAndPartitions);
        return offsetFetchRequest;
    }

    @Override
    public short type() {
        return KafkaCommandKeys.OFFSET_FETCH;
    }
}
