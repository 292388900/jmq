package com.ipd.kafka.handler;

import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.kafka.command.HeartbeatRequest;
import com.ipd.jmq.common.network.kafka.command.HeartbeatResponse;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaHeader;
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
public class HeartbeatHandler extends AbstractHandler implements KafkaHandler {
    private final static Logger logger = LoggerFactory.getLogger(HeartbeatHandler.class);

    private GroupCoordinator coordinator;

    public HeartbeatHandler(GroupCoordinator coordinator) {
        Preconditions.checkArgument(coordinator != null, "GroupCoordinator can't be null");
        this.coordinator = coordinator;
    }

    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {

        if (command == null) {
            return null;
        }

        final HeartbeatRequest heartbeatRequest = (HeartbeatRequest)command.getPayload();
        GroupCoordinator.HeartbeatCallback callback = new GroupCoordinator.HeartbeatCallback() {
            @Override
            public void sendResponseCallback(short errorCode) {
                HeartbeatResponse heartbeatResponse = new HeartbeatResponse();
                heartbeatResponse.setCorrelationId(heartbeatRequest.getCorrelationId());
                heartbeatResponse.setErrorCode(errorCode);
                Command response = new Command(KafkaHeader.Builder.response(command.getHeader().getRequestId()), heartbeatResponse);
                try {
                    transport.acknowledge(command, response, null);
                } catch (TransportException e) {
                    logger.warn("send heartbeat response for {} failed: ", heartbeatRequest.getGroupId(), e);
                }
            }
        };
        // let the coordinator to handle heartbeat
        coordinator.handleHeartbeat(
                heartbeatRequest.getGroupId(),
                heartbeatRequest.getMemberId(),
                heartbeatRequest.getGroupGenerationId(),
                callback);

        return null;
    }

    @Override
    public int[] type() {
        return new int[]{KafkaCommandKeys.HEARTBEAT};
    }
}
