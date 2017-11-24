package com.ipd.kafka.handler;

import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaHeader;
import com.ipd.jmq.common.network.kafka.command.OffsetQueryRequest;
import com.ipd.jmq.common.network.kafka.command.OffsetQueryResponse;
import com.ipd.jmq.common.network.kafka.exception.ErrorCode;
import com.ipd.jmq.common.network.kafka.model.PartitionOffsetsResponse;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
import com.ipd.jmq.server.broker.handler.AbstractHandler;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.kafka.cluster.KafkaClusterManager;
import com.ipd.kafka.mapping.KafkaMapService;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by zhangkepeng on 17-2-16.
 * Modified by luoruiheng
 */
public class OffsetQueryHandler extends AbstractHandler implements KafkaHandler {

    private KafkaClusterManager kafkaClusterManager;
    private DispatchService dispatchService;

    public OffsetQueryHandler(KafkaClusterManager kafkaClusterManager, DispatchService dispatchService) {
        Preconditions.checkArgument(dispatchService != null, "DispatchService can't be null");
        this.dispatchService = dispatchService;
        this.kafkaClusterManager = kafkaClusterManager;
    }

    @Override
    public Command process(Transport transport, Command command) throws TransportException {
        if (command == null) {
            return null;
        }

        OffsetQueryRequest queryRequest = (OffsetQueryRequest)command.getPayload();
        String topic = queryRequest.getTopic();
        int partition = queryRequest.getPartition();
        long timestamp = queryRequest.getTimestamp();
        int maxNumOffsets = queryRequest.getMaxNumOffsets();
        short queue = KafkaMapService.partition2Queue(partition);
        PartitionOffsetsResponse partitionOffsetsResponse = getTopicPartitionOffset(topic, queue, timestamp, maxNumOffsets);
        OffsetQueryResponse queryResponse = new OffsetQueryResponse();
        queryResponse.setCorrelationId(queryRequest.getCorrelationId());
        queryResponse.setPartitionOffsetsResponse(partitionOffsetsResponse);
        Command response = new Command(KafkaHeader.Builder.response(command.getHeader().getRequestId()), queryResponse);

        return response;
    }

    /**
     * 根据分片等信息获取offset
     *
     * @param topic
     * @param queue
     * @param time
     * @return
     */
    private PartitionOffsetsResponse getTopicPartitionOffset(String topic, int queue, long time, int maxNumOffsets) {

        // 返回状态
        short status = ErrorCode.NO_ERROR;
        Set<Long> offsets = new HashSet<Long>();
        /*List<String> consumers = kafkaClusterManager.getConsumerApp(topic);
        long minOffset = Long.MAX_VALUE;
        for (String consumer : consumers) {
            long offset = dispatchService.getOffset(topic, queue, consumer);
            if (offset > -1L && minOffset > offset) {
                minOffset = offset;
            }
        }
        if (minOffset == Long.MAX_VALUE) {
            minOffset = 0L;
        }*/

        Set<Long> jmqOffsets = dispatchService.getOffsetsForKafkaBefore(topic, queue, time, maxNumOffsets);

        // 将JMQ offset映射为Kafka Offset
        for (long jmqOffset : jmqOffsets) {
            offsets.add(KafkaMapService.map2Offset(jmqOffset, KafkaMapService.JMQ_OFFSET_TO_KAFKA_OFFSET));
        }

        PartitionOffsetsResponse partitionOffsetsResponse = new PartitionOffsetsResponse(status, offsets);
        return partitionOffsetsResponse;
    }

    @Override
    public int[] type() {
        return new int[]{KafkaCommandKeys.OFFSET_QUERY};
    }
}
