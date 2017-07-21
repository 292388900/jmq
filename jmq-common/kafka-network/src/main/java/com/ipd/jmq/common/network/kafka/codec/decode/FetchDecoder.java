package com.ipd.jmq.common.network.kafka.codec.decode;

import com.ipd.jmq.common.network.kafka.command.FetchRequest;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangkepeng on 16-8-2.
 */
public class FetchDecoder implements PayloadDecoder<KafkaRequestOrResponse> {

    @Override
    public KafkaRequestOrResponse decode(KafkaRequestOrResponse payload, ByteBuf in) throws Exception {
        if (in == null) {
            return null;
        }

        FetchRequest fetchRequest = (FetchRequest) payload;
        fetchRequest.setVersion(in.readShort());
        fetchRequest.setCorrelationId(in.readInt());
        fetchRequest.setClientId(CommandUtils.readShortString(in));
        fetchRequest.setReplicaId(in.readInt());
        fetchRequest.setMaxWait(in.readInt());
        fetchRequest.setMinBytes(in.readInt());
        int topicCount = in.readInt();
        int numPartitions = 0;
        Map<String, Map<Integer, FetchRequest.PartitionFetchInfo>> topicFetchInfoMap = new HashMap<String, Map<Integer, FetchRequest.PartitionFetchInfo>>();
        for (int i = 0; i < topicCount; i++) {
            String topic = CommandUtils.readShortString(in);
            int partitionCount = in.readInt();
            // 计算partition总数
            numPartitions += partitionCount;
            Map<Integer, FetchRequest.PartitionFetchInfo> partitionFetchInfoMap = new HashMap<Integer, FetchRequest.PartitionFetchInfo>();
            for (int j = 0; j < partitionCount; j++) {
                int partitionId = in.readInt();
                long offset = in.readLong();
                int fetchSize = in.readInt();
                FetchRequest.PartitionFetchInfo partitionFetchInfo = fetchRequest.new PartitionFetchInfo(offset, fetchSize);
                partitionFetchInfoMap.put(partitionId, partitionFetchInfo);
                topicFetchInfoMap.put(topic, partitionFetchInfoMap);
            }
        }
        fetchRequest.setNumPartitions(numPartitions);
        fetchRequest.setRequestInfo(topicFetchInfoMap);
        return fetchRequest;
    }

    @Override
    public short type() {
        return KafkaCommandKeys.FETCH;
    }
}
