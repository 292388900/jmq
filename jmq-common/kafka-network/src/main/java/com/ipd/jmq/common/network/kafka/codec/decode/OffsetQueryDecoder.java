package com.ipd.jmq.common.network.kafka.codec.decode;

import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.command.OffsetQueryRequest;
import com.ipd.jmq.common.network.kafka.command.OffsetQueryResponse;
import com.ipd.jmq.common.network.kafka.model.PartitionOffsetsResponse;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import io.netty.buffer.ByteBuf;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhangkepeng on 17-2-16.
 */
public class OffsetQueryDecoder implements PayloadDecoder<KafkaRequestOrResponse> {

    @Override
    public KafkaRequestOrResponse decode(KafkaRequestOrResponse payload, ByteBuf in) throws TransportException.CodecException{
        if (in == null) {
            return null;
        }

        if (payload instanceof OffsetQueryRequest) {
            OffsetQueryRequest request = (OffsetQueryRequest) payload;
            request.setVersion(in.readShort());
            request.setCorrelationId(in.readInt());
            request.setClientId(CommandUtils.readShortString(in));
            request.setTopic(CommandUtils.readShortString(in));
            request.setPartition(in.readInt());
            request.setTimestamp(in.readLong());
            request.setMaxNumOffsets(in.readInt());
            return request;
        } else if (payload instanceof OffsetQueryResponse) {
            OffsetQueryResponse response = (OffsetQueryResponse) payload;
            response.setCorrelationId(in.readInt());
            short errorCode = in.readShort();
            int size = in.readInt();
            Set<Long> offsets = new HashSet<Long>();
            for (int i = 0; i < size; i++) {
                offsets.add(in.readLong());
            }
            PartitionOffsetsResponse partitionOffsetsResponse = new PartitionOffsetsResponse(errorCode, offsets);
            response.setPartitionOffsetsResponse(partitionOffsetsResponse);
            return response;
        } else {
            return null;
        }
    }

    @Override
    public short type() {
        return KafkaCommandKeys.OFFSET_QUERY;
    }
}
