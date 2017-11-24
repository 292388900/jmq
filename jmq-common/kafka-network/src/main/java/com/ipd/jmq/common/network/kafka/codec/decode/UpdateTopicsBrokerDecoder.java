package com.ipd.jmq.common.network.kafka.codec.decode;

import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.command.UpdateTopicsBrokerRequest;
import com.ipd.jmq.common.network.kafka.command.UpdateTopicsBrokerResponse;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import com.ipd.jmq.common.network.TransportException;
import io.netty.buffer.ByteBuf;

/**
 * Created by zhangkepeng on 16-9-6.
 */
public class UpdateTopicsBrokerDecoder implements PayloadDecoder<KafkaRequestOrResponse>{

    @Override
    public KafkaRequestOrResponse decode(KafkaRequestOrResponse payload, ByteBuf in) throws TransportException.CodecException {
        if (in == null) {
            return null;
        }

        if (payload instanceof UpdateTopicsBrokerRequest) {
            UpdateTopicsBrokerRequest request = (UpdateTopicsBrokerRequest) payload;
            request.setVersion(in.readShort());
            request.setCorrelationId(in.readInt());
            request.setClientId(CommandUtils.readShortString(in));
            String topic = CommandUtils.readShortString(in);
            request.setPartition(in.readInt());
            request.setTopicsBrokerType(in.readInt());
            request.setLastBrokerId(in.readInt());
            request.setControllerEpoch(in.readInt());
            request.setTopic(topic);
            return request;
        } else if (payload instanceof UpdateTopicsBrokerResponse) {
            UpdateTopicsBrokerResponse response = (UpdateTopicsBrokerResponse) payload;
            response.setCorrelationId(in.readInt());
            response.setTopic(CommandUtils.readShortString(in));
            response.setErrorCode(in.readShort());
            return response;
        } else {
            return null;
        }
    }

    @Override
    public short type() {
        return KafkaCommandKeys.UPDATE_TOPICS_BROKER;
    }
}
