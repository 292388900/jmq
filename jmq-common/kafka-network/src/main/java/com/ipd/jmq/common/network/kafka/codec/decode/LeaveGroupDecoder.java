package com.ipd.jmq.common.network.kafka.codec.decode;

import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.command.LeaveGroupRequest;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import io.netty.buffer.ByteBuf;

/**
 * Created by zhangkepeng on 17-2-10.
 */
public class LeaveGroupDecoder implements PayloadDecoder<KafkaRequestOrResponse> {

    @Override
    public KafkaRequestOrResponse decode(KafkaRequestOrResponse payload, ByteBuf in) throws TransportException.CodecException {
        if (in == null) {
            return null;
        }

        LeaveGroupRequest request = (LeaveGroupRequest) payload;
        request.setVersion(in.readShort());
        request.setCorrelationId(in.readInt());
        request.setClientId(CommandUtils.readShortString(in));
        request.setGroupId(CommandUtils.readShortString(in));
        request.setMemberId(CommandUtils.readShortString(in));
        return request;
    }

    @Override
    public short type() {
        return KafkaCommandKeys.LEAVE_GROUP;
    }
}
