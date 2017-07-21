package com.ipd.jmq.common.network.kafka.command;

import com.ipd.jmq.common.network.kafka.exception.KafkaException;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.common.network.v3.command.CommandFactory;
import com.ipd.jmq.common.network.v3.command.Direction;
import com.ipd.jmq.common.network.v3.command.Header;

/**
 * Created by zhangkepeng on 16-8-1.
 */
public class KafkaCommandFactory implements CommandFactory {

    @Override
    public Command create(Header header) {
        if (header == null) {
            return null;
        }

        Object payload = null;

        KafkaHeader kafkaHeader = (KafkaHeader) header;
        switch (kafkaHeader.getCommandKey()) {
            case KafkaCommandKeys.PRODUCE:
                payload = new ProduceRequest();
                break;
            case KafkaCommandKeys.FETCH:
                payload = new FetchRequest();
                break;
            case KafkaCommandKeys.LIST_OFFSETS:
                payload = new OffsetRequest();
                break;
            case KafkaCommandKeys.METADATA:
                payload = new TopicMetadataRequest();
                break;
            case KafkaCommandKeys.OFFSET_COMMIT:
                payload = new OffsetCommitRequest();
                break;
            case KafkaCommandKeys.OFFSET_FETCH:
                payload = new OffsetFetchRequest();
                break;
            case KafkaCommandKeys.GROUP_COORDINATOR:
                payload = new GroupCoordinatorRequest();
                break;
            case KafkaCommandKeys.JOIN_GROUP:
                payload = new JoinGroupRequest();
                break;
            case KafkaCommandKeys.HEARTBEAT:
                payload = new HeartbeatRequest();
                break;
            case KafkaCommandKeys.LEAVE_GROUP:
                payload = new LeaveGroupRequest();
                break;
            case KafkaCommandKeys.SYNC_GROUP:
                payload = new SyncGroupRequest();
                break;
            case KafkaCommandKeys.UPDATE_TOPICS_BROKER:
                Direction updateTopicsBrokerDirection = kafkaHeader.getDirection();
                if (updateTopicsBrokerDirection == Direction.REQUEST) {
                    payload = new UpdateTopicsBrokerRequest();
                } else if (updateTopicsBrokerDirection == Direction.RESPONSE) {
                    payload = new UpdateTopicsBrokerResponse();
                }
                break;
            case KafkaCommandKeys.OFFSET_QUERY:
                Direction offsetQueryDirection = kafkaHeader.getDirection();
                if (offsetQueryDirection == Direction.REQUEST) {
                    payload = new OffsetQueryRequest();
                } else if (offsetQueryDirection == Direction.RESPONSE) {
                    payload = new OffsetQueryResponse();
                }
                break;
            default:
                break;
                //throw new KafkaException("not support command, commandKey=" + kafkaHeader.getCommandKey());
        }

        return new Command(header, payload);
    }
}
