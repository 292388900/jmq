package com.ipd.kafka.handler;

import com.google.common.base.Preconditions;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaHeader;
import com.ipd.jmq.common.network.kafka.command.UpdateTopicsBrokerRequest;
import com.ipd.jmq.common.network.kafka.command.UpdateTopicsBrokerResponse;
import com.ipd.jmq.server.broker.handler.AbstractHandler;
import com.ipd.kafka.cluster.KafkaClusterEvent;
import com.ipd.kafka.cluster.KafkaClusterManager;
import com.ipd.kafka.cluster.MetadataUpdater;
import com.ipd.kafka.cluster.TopicsBrokerEvent;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.v3.command.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhangkepeng on 16-8-24.
 * Modified by luoruiheng
 */
public class UpdateTopicsBrokerHandler extends AbstractHandler implements KafkaHandler {
    private static final Logger logger = LoggerFactory.getLogger(UpdateTopicsBrokerHandler.class);

    private KafkaClusterManager kafkaClusterManager;

    private MetadataUpdater metadataUpdater;

    public UpdateTopicsBrokerHandler(KafkaClusterManager kafkaClusterManager) {
        Preconditions.checkArgument(kafkaClusterManager != null, "KafkaClusterManager can't be null");
        this.kafkaClusterManager = kafkaClusterManager;
        this.metadataUpdater = kafkaClusterManager.metadataUpdater;
        Preconditions.checkArgument(metadataUpdater != null, "MetadataUpdater can't be null");
    }

    @Override
    public Command process(Transport transport, Command command) throws TransportException {
        if (command == null) {
            return null;
        }
        Command response = null;
        UpdateTopicsBrokerRequest updateTopicsBrokerRequest = (UpdateTopicsBrokerRequest)command.getPayload();
        String topic = updateTopicsBrokerRequest.getTopic();
        int partition = updateTopicsBrokerRequest.getPartition();
        int topicsBrokerType = updateTopicsBrokerRequest.getTopicsBrokerType();
        int lastBrokerId = updateTopicsBrokerRequest.getLastBrokerId();
        int correlationId = updateTopicsBrokerRequest.getCorrelationId();
        int controllerEpoch = updateTopicsBrokerRequest.getControllerEpoch();
        // 构造更新partition事件
        TopicsBrokerEvent topicsBrokerEvent = new TopicsBrokerEvent.Builder(topic, KafkaClusterEvent.EventType.valueOf(topicsBrokerType)).build();
        topicsBrokerEvent.setPartition(partition);
        topicsBrokerEvent.setLastBrokerId(lastBrokerId);
        topicsBrokerEvent.setControllerEpoch(controllerEpoch);
        short status = metadataUpdater.addPartitionUpdateEvent(topicsBrokerEvent);
        UpdateTopicsBrokerResponse updateTopicsBrokerResponse = new UpdateTopicsBrokerResponse();
        updateTopicsBrokerResponse.setTopic(topic);
        updateTopicsBrokerResponse.setCorrelationId(correlationId);
        updateTopicsBrokerResponse.setErrorCode(status);
        response = new Command(KafkaHeader.Builder.response(command.getHeader().getRequestId()), updateTopicsBrokerResponse);
        return response;
    }

    @Override
    public int[] type() {
        return new int[]{KafkaCommandKeys.UPDATE_TOPICS_BROKER};
    }
}
