package com.ipd.kafka.handler;

import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.kafka.command.GroupCoordinatorRequest;
import com.ipd.jmq.common.network.kafka.command.GroupCoordinatorResponse;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaHeader;
import com.ipd.jmq.common.network.kafka.exception.ErrorCode;
import com.ipd.jmq.common.network.kafka.model.DelayedResponseKey;
import com.ipd.jmq.common.network.kafka.model.KafkaBroker;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.server.broker.handler.AbstractHandler;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.kafka.DelayedResponseManager;
import com.ipd.kafka.cluster.KafkaClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhangkepeng on 17-2-13.
 * Modified by luoruiheng
 */
public class GroupCoordinatorHandler extends AbstractHandler implements KafkaHandler {
    private final static Logger logger = LoggerFactory.getLogger(GroupCoordinatorHandler.class);

    private KafkaClusterManager kafkaClusterManager;
    private DelayedResponseManager delayedResponseManager;

    public GroupCoordinatorHandler(DelayedResponseManager delayedResponseManager, KafkaClusterManager kafkaClusterManager) {
        Preconditions.checkArgument(delayedResponseManager != null, "DelayedResponseManager can't be null");
        Preconditions.checkArgument(kafkaClusterManager != null, "KafkaClusterManager can't be null");
        this.kafkaClusterManager = kafkaClusterManager;
        this.delayedResponseManager = delayedResponseManager;
    }

    @Override
    public Command process(Transport transport, Command command) throws TransportException {

        if (command == null) {
            return null;
        }
        Command response = null;
        GroupCoordinatorRequest groupCoordinatorRequest = (GroupCoordinatorRequest) command.getPayload();
        KafkaBroker kafkaBroker = kafkaClusterManager.getKafkaBroker();
        GroupCoordinatorResponse groupCoordinatorResponse = new GroupCoordinatorResponse();
        groupCoordinatorResponse.setCorrelationId(groupCoordinatorRequest.getCorrelationId());

        final String groupId = groupCoordinatorRequest.getGroupId();
        if (null == groupId || groupId.isEmpty()) {
            kafkaBroker = new KafkaBroker(-1, "", -1);
            groupCoordinatorResponse.setErrorCode(ErrorCode.INVALID_GROUP_ID);
        } else {
            if (kafkaBroker != null && kafkaBroker.getId() != -1) {
                groupCoordinatorResponse.setErrorCode(ErrorCode.NO_ERROR);
            } else {
                kafkaBroker = new KafkaBroker(-1, "", -1);
                groupCoordinatorResponse.setErrorCode(ErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE);
            }
        }
        groupCoordinatorResponse.setKafkaBroker(kafkaBroker);

        response = new Command(KafkaHeader.Builder.response(command.getHeader().getRequestId()), groupCoordinatorResponse);
        /*DelayedResponseKey delayedResponseKey = new DelayedResponseKey(transport, command, response, DelayedResponseKey.Type.COORDINATOR, 0L);
        if (!delayedResponseManager.suspend(delayedResponseKey)) {
            delayedResponseManager.closeTransportAndClearQueue(transport);
        }*/
        return response;
    }

    @Override
    public int[] type() {
        return new int[]{KafkaCommandKeys.GROUP_COORDINATOR};
    }
}
