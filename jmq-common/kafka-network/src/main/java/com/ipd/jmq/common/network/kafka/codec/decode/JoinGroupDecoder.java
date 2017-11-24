package com.ipd.jmq.common.network.kafka.codec.decode;

import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.kafka.command.JoinGroupRequest;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangkepeng on 17-2-10.
 */
public class JoinGroupDecoder implements PayloadDecoder<KafkaRequestOrResponse> {

    @Override
    public KafkaRequestOrResponse decode(KafkaRequestOrResponse payload, ByteBuf in) throws TransportException.CodecException {
        if (in == null) {
            return null;
        }

        JoinGroupRequest request = (JoinGroupRequest)payload;
        request.setVersion(in.readShort());
        request.setCorrelationId(in.readInt());
        request.setClientId(CommandUtils.readShortString(in));
        request.setGroupId(CommandUtils.readShortString(in));
        request.setSessionTimeout(in.readInt());
        request.setMemberId(CommandUtils.readShortString(in));
        request.setProtocolType(CommandUtils.readShortString(in));
        int size = in.readInt();
        List<JoinGroupRequest.ProtocolMetadata> groupProtocols = null;
        if (size > 0) {
            groupProtocols = new ArrayList<JoinGroupRequest.ProtocolMetadata>(size);
        }
        for (int i = 0; i < size; i++) {
            String groupName = CommandUtils.readShortString(in);
            int length = in.readInt();
            ByteBuf byteBuf = in.readBytes(length);
            byte[] bytes;
            if (byteBuf.hasArray()) {
                bytes = byteBuf.array();
            } else {
                bytes = new byte[length];
                byteBuf.getBytes(byteBuf.readerIndex(), bytes);
            }
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            byteBuffer.rewind();
            JoinGroupRequest.ProtocolMetadata protocolMetadata = new JoinGroupRequest.ProtocolMetadata(groupName, byteBuffer);
            groupProtocols.add(protocolMetadata);
        }
        request.setGroupProtocols(groupProtocols);
        return request;
    }

    @Override
    public short type() {
        return KafkaCommandKeys.JOIN_GROUP;
    }

}
