package com.ipd.kafka.handler;

import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaHeader;
import com.ipd.jmq.common.network.kafka.command.SyncGroupRequest;
import com.ipd.jmq.common.network.kafka.command.SyncGroupResponse;
import com.ipd.jmq.common.network.kafka.utils.Utils;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.server.broker.handler.AbstractHandler;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.kafka.coordinator.GroupCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangkepeng on 17-2-14.
 * Modified by luoruiheng
 */
public class SyncGroupHandler extends AbstractHandler implements KafkaHandler {
    private final static Logger logger = LoggerFactory.getLogger(SyncGroupHandler.class);

    private GroupCoordinator coordinator;

    public SyncGroupHandler(GroupCoordinator coordinator) {
        Preconditions.checkArgument(coordinator != null, "GroupCoordinator can't be null");
        this.coordinator = coordinator;
    }

    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {

        if (command == null) {
            return null;
        }
        final SyncGroupRequest syncGroupRequest = (SyncGroupRequest)command.getPayload();

        GroupCoordinator.SyncCallback callback = new GroupCoordinator.SyncCallback() {
            @Override
            public void sendResponseCallback(byte[] assignment, short errorCode) {
                SyncGroupResponse syncGroupResponse = new SyncGroupResponse();
                syncGroupResponse.setCorrelationId(syncGroupRequest.getCorrelationId());
                if (assignment != null) {
                    syncGroupResponse.setMemberState(ByteBuffer.wrap(assignment));
                } else {
                    syncGroupResponse.setMemberState(null);
                }
                syncGroupResponse.setErrorCode(errorCode);
                Command response = new Command(KafkaHeader.Builder.response(command.getHeader().getRequestId()), syncGroupResponse);
                try {
                    transport.acknowledge(command, response, null);
                } catch (TransportException e) {
                    logger.warn("send sync group response for {} failed: ", syncGroupRequest.getGroupId(), e);
                }
            }
        };

        Map<String, ByteBuffer> assignments = syncGroupRequest.getGroupAssignment();
        Map<String, byte[]> assignmentMap = new HashMap<String, byte[]>();

        if (null != assignments && !assignments.isEmpty()) {
            for (Map.Entry<String, ByteBuffer> entry : assignments.entrySet()) {
                String memberId = entry.getKey();
                ByteBuffer assignment = entry.getValue();
                assignmentMap.put(memberId, Utils.toArray(assignment));
            }
        }

        coordinator.handleSyncGroup(
                syncGroupRequest.getGroupId(),
                syncGroupRequest.getGenerationId(),
                syncGroupRequest.getMemberId(),
                assignmentMap,
                callback);

        return null;
    }

    @Override
    public int[] type() {
        return new int[]{KafkaCommandKeys.SYNC_GROUP};
    }
}
