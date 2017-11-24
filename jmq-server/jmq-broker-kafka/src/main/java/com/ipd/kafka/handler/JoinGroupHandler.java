package com.ipd.kafka.handler;

import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.kafka.command.JoinGroupRequest;
import com.ipd.jmq.common.network.kafka.command.JoinGroupResponse;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaHeader;
import com.ipd.jmq.common.network.kafka.utils.Utils;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.server.broker.handler.AbstractHandler;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.kafka.coordinator.GroupCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangkepeng on 17-2-13.
 * Modified by luoruiheng
 */
public class JoinGroupHandler extends AbstractHandler implements KafkaHandler {
    private final static Logger logger = LoggerFactory.getLogger(JoinGroupHandler.class);

    private GroupCoordinator coordinator;

    public JoinGroupHandler(GroupCoordinator coordinator) {
        Preconditions.checkArgument(coordinator != null, "GroupCoordinator can't be null");
        this.coordinator = coordinator;
    }

    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {

        if (command == null) {
            return null;
        }
        final JoinGroupRequest joinGroupRequest = (JoinGroupRequest)command.getPayload();
        GroupCoordinator.JoinCallback callback = new GroupCoordinator.JoinCallback() {
            @Override
            public void sendResponseCallback(GroupCoordinator.JoinGroupResult joinGroupResult) {

                Map<String, byte[]> memberMap = joinGroupResult.getMembers();
                Map<String, ByteBuffer> members = new HashMap<String, ByteBuffer>();
                if (memberMap != null) {
                    for (Map.Entry<String, byte[]> entry : memberMap.entrySet()) {
                        members.put(entry.getKey(), ByteBuffer.wrap(entry.getValue()));
                    }
                }
                JoinGroupResponse joinGroupResponse = new JoinGroupResponse();
                joinGroupResponse.setCorrelationId(joinGroupRequest.getCorrelationId());
                joinGroupResponse.setErrorCode(joinGroupResult.getErrorCode());
                joinGroupResponse.setGenerationId(joinGroupResult.getGenerationId());
                joinGroupResponse.setGroupProtocol(joinGroupResult.getSubProtocol());
                joinGroupResponse.setMemberId(joinGroupResult.getMemberId());
                joinGroupResponse.setLeaderId(joinGroupResult.getLeaderId());
                joinGroupResponse.setMembers(members);
                Command response = new Command(KafkaHeader.Builder.response(command.getHeader().getRequestId()), joinGroupResponse);
                try {
                    transport.acknowledge(command, response, null);
                } catch (TransportException e) {
                    logger.warn("send join group response for {} failed: ", joinGroupRequest.getGroupId(), e);
                }
            }
        };

        List<JoinGroupRequest.ProtocolMetadata> protocolMetadatas = joinGroupRequest.getGroupProtocols();
        Map<String, byte[]> protocols = new HashMap<String, byte[]>();
        if (protocolMetadatas != null) {
            for (JoinGroupRequest.ProtocolMetadata protocolMetadata : protocolMetadatas) {
                String protocolName = protocolMetadata.name();
                byte[] protocolMata = Utils.toArray(protocolMetadata.metadata());
                protocols.put(protocolName, protocolMata);
            }
        }

        coordinator.handleJoinGroup(
                joinGroupRequest.getGroupId(),
                joinGroupRequest.getMemberId(),
                joinGroupRequest.getClientId(),
                transport.remoteAddress().toString(),
                joinGroupRequest.getSessionTimeout(),
                joinGroupRequest.getProtocolType(),
                protocols,
                callback);
        return null;
    }

    @Override
    public int[] type() {
        return new int[]{KafkaCommandKeys.JOIN_GROUP};
    }
}
