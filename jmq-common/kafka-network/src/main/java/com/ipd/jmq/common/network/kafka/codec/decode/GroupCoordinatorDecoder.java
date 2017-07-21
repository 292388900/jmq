package com.ipd.jmq.common.network.kafka.codec.decode;

import com.ipd.jmq.common.network.kafka.command.GroupCoordinatorRequest;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import com.ipd.jmq.common.network.TransportException;
import io.netty.buffer.ByteBuf;

/**
 * Created by zhangkepeng on 16-8-16.
 */
public class GroupCoordinatorDecoder implements PayloadDecoder<KafkaRequestOrResponse> {

    @Override
    public KafkaRequestOrResponse decode(KafkaRequestOrResponse payload, ByteBuf in) throws TransportException.CodecException {
        if (in == null) {
            return null;
        }

        GroupCoordinatorRequest request = (GroupCoordinatorRequest) payload;
        request.setVersion(in.readShort());
        request.setCorrelationId(in.readInt());
        request.setClientId(CommandUtils.readShortString(in));
        request.setGroupId(CommandUtils.readShortString(in));

        return request;
    }

    @Override
    public short type() {
        return KafkaCommandKeys.GROUP_COORDINATOR;
    }

}
