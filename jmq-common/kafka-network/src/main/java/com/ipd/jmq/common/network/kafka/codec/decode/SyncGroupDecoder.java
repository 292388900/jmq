package com.ipd.jmq.common.network.kafka.codec.decode;

import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.command.SyncGroupRequest;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangkepeng on 17-2-10.
 */
public class SyncGroupDecoder implements PayloadDecoder<KafkaRequestOrResponse> {

    @Override
    public KafkaRequestOrResponse decode(KafkaRequestOrResponse payload, ByteBuf in) throws TransportException.CodecException {
        if (in == null) {
            return null;
        }

        SyncGroupRequest request = (SyncGroupRequest) payload;
        request.setVersion(in.readShort());
        request.setCorrelationId(in.readInt());
        request.setClientId(CommandUtils.readShortString(in));
        request.setGroupId(CommandUtils.readShortString(in));
        request.setGenerationId(in.readInt());
        request.setMemberId(CommandUtils.readShortString(in));

        int size = in.readInt();
        Map<String, ByteBuffer> assignment = null;
        if (size > 0) {
            assignment = new HashMap<String, ByteBuffer>();
        }
        for (int i = 0; i < size; i++) {
            String memberId = CommandUtils.readShortString(in);
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
            assignment.put(memberId, byteBuffer);
        }
        request.setGroupAssignment(assignment);
        return request;
    }

    @Override
    public short type() {
        return KafkaCommandKeys.SYNC_GROUP;
    }
}
