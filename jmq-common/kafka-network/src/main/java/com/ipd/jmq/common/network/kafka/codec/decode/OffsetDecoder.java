package com.ipd.jmq.common.network.kafka.codec.decode;

import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.command.OffsetRequest;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import com.ipd.jmq.common.network.TransportException;
import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangkepeng on 16-8-2.
 */
public class OffsetDecoder implements PayloadDecoder<KafkaRequestOrResponse> {

    @Override
    public KafkaRequestOrResponse decode(KafkaRequestOrResponse payload, ByteBuf in) throws TransportException.CodecException{
        if (in == null) {
            return null;
        }

        OffsetRequest offsetRequest = (OffsetRequest) payload;
        offsetRequest.setVersion(in.readShort());
        offsetRequest.setCorrelationId(in.readInt());
        offsetRequest.setClientId(CommandUtils.readShortString(in));
        offsetRequest.setReplicaId(in.readInt());
        int topicCount = in.readInt();
        Map<String, Map<Integer, OffsetRequest.PartitionOffsetRequestInfo>> offsetRequestMap = new HashMap<String, Map<Integer, OffsetRequest.PartitionOffsetRequestInfo>>();
        for (int i = 0; i < topicCount; i++) {
            String topic = CommandUtils.readShortString(in);
            int partitionCount = in.readInt();
            for (int j = 0; j < partitionCount; j++) {
                int partitionId = in.readInt();
                long time = in.readLong();
                int maxNumOffsets = in.readInt();
                Map<Integer, OffsetRequest.PartitionOffsetRequestInfo> offsetInfoMap = new HashMap<Integer, OffsetRequest.PartitionOffsetRequestInfo>();
                OffsetRequest.PartitionOffsetRequestInfo partitionOffsetRequestInfo = offsetRequest.new PartitionOffsetRequestInfo(time, maxNumOffsets);
                offsetInfoMap.put(partitionId, partitionOffsetRequestInfo);
                offsetRequestMap.put(topic, offsetInfoMap);
            }
        }
        offsetRequest.setOffsetRequestMap(offsetRequestMap);
        return offsetRequest;
    }

    @Override
    public short type() {
        return KafkaCommandKeys.LIST_OFFSETS;
    }
}
