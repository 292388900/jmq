package com.ipd.kafka.handler;

import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaHeader;
import com.ipd.jmq.common.network.kafka.command.LeaveGroupRequest;
import com.ipd.jmq.common.network.kafka.command.LeaveGroupResponse;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.server.broker.handler.AbstractHandler;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.kafka.coordinator.GroupCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhangkepeng on 17-2-14.
 * Modified by luoruiheng
 */
public class LeaveGroupHandler extends AbstractHandler implements KafkaHandler {
    private final static Logger logger = LoggerFactory.getLogger(LeaveGroupHandler.class);

    private GroupCoordinator coordinator;

    public LeaveGroupHandler(GroupCoordinator coordinator) {
        Preconditions.checkArgument(coordinator != null, "GroupCoordinator can't be null");
        this.coordinator = coordinator;
    }

    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {

        if (command == null) {
            return null;
        }
        final LeaveGroupRequest leaveGroupRequest = (LeaveGroupRequest)command.getPayload();
        GroupCoordinator.LeaveCallback callback = new GroupCoordinator.LeaveCallback() {
            @Override
            public void sendResponseCallback(short errorCode) {
                LeaveGroupResponse leaveGroupResponse = new LeaveGroupResponse();
                leaveGroupResponse.setCorrelationId(leaveGroupRequest.getCorrelationId());
                leaveGroupResponse.setErrorCode(errorCode);
                Command response = new Command(KafkaHeader.Builder.response(command.getHeader().getRequestId()), leaveGroupResponse);
                try {
                    transport.acknowledge(command, response, null);
                } catch (TransportException e) {
                    logger.warn("send leave group response for {} failed: ", leaveGroupRequest.getGroupId(), e);
                }
            }
        };

        // let the coordinator to handle leave-group
        coordinator.handleLeaveGroup(
                leaveGroupRequest.getGroupId(),
                leaveGroupRequest.getMemberId(),
                callback);

        return null;
    }

    @Override
    public int[] type() {
        return new int[]{KafkaCommandKeys.LEAVE_GROUP};
    }
}
