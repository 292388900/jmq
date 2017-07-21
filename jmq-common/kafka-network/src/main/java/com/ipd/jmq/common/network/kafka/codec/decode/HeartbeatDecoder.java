package com.ipd.jmq.common.network.kafka.codec.decode;

import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.kafka.command.HeartbeatRequest;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import io.netty.buffer.ByteBuf;

/**
 * Created by zhangkepeng on 17-2-10.
 */
public class HeartbeatDecoder implements PayloadDecoder<KafkaRequestOrResponse> {

    @Override
    public KafkaRequestOrResponse decode(KafkaRequestOrResponse payload, ByteBuf in) throws TransportException.CodecException {
        if (in == null) {
            return null;
        }

        HeartbeatRequest request = (HeartbeatRequest)payload;
        request.setVersion(in.readShort());
        request.setCorrelationId(in.readInt());
        request.setClientId(CommandUtils.readShortString(in));
        request.setGroupId(CommandUtils.readShortString(in));
        request.setGroupGenerationId(in.readInt());
        request.setMemberId(CommandUtils.readShortString(in));
        return request;
    }

    @Override
    public short type() {
        return KafkaCommandKeys.HEARTBEAT;
    }
}
