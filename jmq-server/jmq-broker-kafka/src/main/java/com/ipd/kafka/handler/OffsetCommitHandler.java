package com.ipd.kafka.handler;

import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaHeader;
import com.ipd.jmq.common.network.kafka.command.OffsetCommitRequest;
import com.ipd.jmq.common.network.kafka.command.OffsetCommitResponse;
import com.ipd.jmq.common.network.kafka.exception.ErrorCode;
import com.ipd.jmq.server.broker.handler.AbstractHandler;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
import com.ipd.kafka.coordinator.GroupCoordinator;
import com.ipd.kafka.mapping.KafkaMapService;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.registry.Registry;
import com.ipd.jmq.toolkit.lang.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by zhangkepeng on 16-7-29.
 * Modified by luoruiheng
 */
public class OffsetCommitHandler extends AbstractHandler implements KafkaHandler {
    private final static Logger logger = LoggerFactory.getLogger(OffsetCommitHandler.class);

    private GroupCoordinator coordinator;

    public OffsetCommitHandler(GroupCoordinator coordinator) {
        Preconditions.checkArgument(coordinator != null, "GroupCoordinator can't be null");
        this.coordinator = coordinator;
    }

    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {

        if (command == null) {
            return null;
        }
        final OffsetCommitRequest offsetCommitRequest = (OffsetCommitRequest) command.getPayload();
        int version = offsetCommitRequest.getVersion();
        // 0.8 version can also submit offset to brokers
        Preconditions.checkState(version >= 1);

        // the callback for sending an offset commit response
        GroupCoordinator.CommitCallback commitCallback = new GroupCoordinator.CommitCallback() {
            @Override
            public void sendResponseCallback(Map<String, Map<Integer, Short>> commitStatus) {

                for (Map.Entry<String, Map<Integer, Short>> commitEntry : commitStatus.entrySet()) {
                    if (logger.isDebugEnabled()) {
                        String topic = commitEntry.getKey();
                        Map<Integer, Short> errorCodes = commitEntry.getValue();
                        for (Map.Entry<Integer, Short> codeEntry : errorCodes.entrySet()) {
                            if (codeEntry.getValue() != ErrorCode.NO_ERROR) {
                                logger.debug("Offset commit request with correlation id {} from client {} on partition {} failed due to {}",
                                        offsetCommitRequest.getGroupId(),
                                        offsetCommitRequest.getClientId(),
                                        topic,
                                        codeEntry.getValue());
                            }
                        }
                    }

                    OffsetCommitResponse offsetCommitResponse = new OffsetCommitResponse();
                    offsetCommitResponse.setCorrelationId(offsetCommitRequest.getCorrelationId());
                    offsetCommitResponse.setCommitStatus(commitStatus);
                    Command response = new Command(KafkaHeader.Builder.response(command.getHeader().getRequestId()),
                            offsetCommitResponse);
                    try {
                        transport.acknowledge(command, response, null);
                    } catch (TransportException e) {
                        logger.warn("send commit offset response for {} failed: ", offsetCommitRequest.getGroupId(), e);
                    }
                }
            }
        };

        // call coordinator to handle commit offset
        coordinator.handleCommitOffsets(
                offsetCommitRequest.getGroupId(),
                offsetCommitRequest.getMemberId(),
                offsetCommitRequest.getGroupGenerationId(),
                offsetCommitRequest.getOffsetAndMetadataMap(),
                commitCallback);

        return null;
    }

    @Override
    public int[] type() {
        return new int[]{KafkaCommandKeys.OFFSET_COMMIT};
    }
}
