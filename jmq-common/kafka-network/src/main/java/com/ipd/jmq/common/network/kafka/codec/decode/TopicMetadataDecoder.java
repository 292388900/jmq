package com.ipd.jmq.common.network.kafka.codec.decode;

import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.command.TopicMetadataRequest;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import com.ipd.jmq.common.network.TransportException;
import io.netty.buffer.ByteBuf;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhangkepeng on 16-8-2.
 */
public class TopicMetadataDecoder implements PayloadDecoder<KafkaRequestOrResponse> {

    @Override
    public KafkaRequestOrResponse decode(KafkaRequestOrResponse payload, ByteBuf in) throws TransportException.CodecException {
        if (in == null) {
            return null;
        }

        TopicMetadataRequest topicMetadataRequest = (TopicMetadataRequest) payload;
        topicMetadataRequest.setVersion(in.readShort());
        topicMetadataRequest.setCorrelationId(in.readInt());
        topicMetadataRequest.setClientId(CommandUtils.readShortString(in));
        int numTopics = CommandUtils.readIntInRange(in, "number of topics", 0, Integer.MAX_VALUE);
        Set<String> topics = new HashSet<String>();
        for (int i = 0; i < numTopics; i++) {
            topics.add(CommandUtils.readShortString(in));
        }
        topicMetadataRequest.setTopics(topics);
        return topicMetadataRequest;
    }

    @Override
    public short type() {
        return KafkaCommandKeys.METADATA;
    }
}
