package com.ipd.kafka.handler;

import com.google.common.collect.HashMultimap;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaHeader;
import com.ipd.jmq.common.network.kafka.command.OffsetFetchRequest;
import com.ipd.jmq.common.network.kafka.command.OffsetFetchResponse;
import com.ipd.jmq.common.network.kafka.exception.ErrorCode;
import com.ipd.jmq.common.network.kafka.model.OffsetMetadataAndError;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
import com.ipd.jmq.server.broker.handler.AbstractHandler;
import com.ipd.kafka.coordinator.GroupCoordinator;
import com.ipd.kafka.mapping.KafkaMapService;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.registry.Registry;
import com.ipd.jmq.toolkit.lang.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhangkepeng on 16-7-29.
 * Modified by luoruiheng
 */
public class OffsetFetchHandler extends AbstractHandler implements KafkaHandler {

    private GroupCoordinator coordinator;

    public OffsetFetchHandler(GroupCoordinator coordinator) {
        Preconditions.checkArgument(coordinator != null, "GroupCoordinator can't be null");
        this.coordinator = coordinator;
    }

    @Override
    public Command process(Transport transport, Command command) throws TransportException {
        if (command == null) {
            return null;
        }
        OffsetFetchRequest offsetFetchRequest = (OffsetFetchRequest) command.getPayload();
        String groupId = offsetFetchRequest.getGroupId();
        HashMultimap<String, Integer> topicAndPartitions = offsetFetchRequest.getTopicAndPartitions();
        Map<String, Map<Integer, OffsetMetadataAndError>> topicPartitionOffsetMetadataMap =
                coordinator.handleFetchOffsets(groupId, topicAndPartitions);
        OffsetFetchResponse offsetFetchResponse = new OffsetFetchResponse();
        offsetFetchResponse.setTopicMetadataAndErrors(topicPartitionOffsetMetadataMap);
        offsetFetchResponse.setCorrelationId(offsetFetchRequest.getCorrelationId());
        Command response = new Command(KafkaHeader.Builder.response(command.getHeader().getRequestId()), offsetFetchResponse);
        return response;
    }

    @Override
    public int[] type() {
        return new int[]{KafkaCommandKeys.OFFSET_FETCH};
    }
}
