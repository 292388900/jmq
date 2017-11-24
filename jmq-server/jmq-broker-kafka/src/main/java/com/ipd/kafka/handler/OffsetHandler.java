package com.ipd.kafka.handler;

import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaHeader;
import com.ipd.jmq.common.network.kafka.command.OffsetRequest;
import com.ipd.jmq.common.network.kafka.command.OffsetResponse;
import com.ipd.jmq.common.network.kafka.model.PartitionOffsetsResponse;
import com.ipd.jmq.server.broker.handler.AbstractHandler;
import com.ipd.kafka.mapping.KafkaMapService;
import com.ipd.jmq.server.store.Store;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.common.network.kafka.model.DelayedResponseKey;
import com.ipd.kafka.DelayedResponseManager;
import com.ipd.kafka.coordinator.GroupCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by zhangkepeng on 16-7-29.
 * Modified by luoruiheng
 */
public class OffsetHandler extends AbstractHandler implements KafkaHandler {
    private final static Logger logger = LoggerFactory.getLogger(OffsetHandler.class);

    private GroupCoordinator coordinator;
    private DelayedResponseManager delayedResponseManager;

    public OffsetHandler(DelayedResponseManager delayedResponseManager, GroupCoordinator coordinator) {
        Preconditions.checkArgument(delayedResponseManager != null, "DelayedResponseManager can't be null");
        Preconditions.checkArgument(coordinator != null, "GroupCoordinator can't be null");
        this.coordinator = coordinator;
        this.delayedResponseManager = delayedResponseManager;
    }

    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {
        if (command == null) {
            return null;
        }
        final OffsetRequest offsetRequest = (OffsetRequest) command.getPayload();
        final Map<String, Map<Integer, OffsetRequest.PartitionOffsetRequestInfo>> offsetRequestMap = offsetRequest.getOffsetRequestMap();
        GroupCoordinator.QueryCallback queryCallback = new GroupCoordinator.QueryCallback() {
            @Override
            public void sendResponseCallback(Map<String, Map<Integer, PartitionOffsetsResponse>> topicPartitionOffsets) {
                OffsetResponse offsetResponse = new OffsetResponse();
                offsetResponse.setCorrelationId(offsetRequest.getCorrelationId());
                offsetResponse.setOffsetsResponseMap(topicPartitionOffsets);
                Command response = new Command(KafkaHeader.Builder.response(command.getHeader().getRequestId()), offsetResponse);

                /*DelayedResponseKey delayedResponseKey = new DelayedResponseKey(transport, command, response, DelayedResponseKey.Type.OFFSET, 5000L);
                if (!delayedResponseManager.suspend(delayedResponseKey)) {
                    // 入队失败，无法保证顺序返回，删除队列数据，关闭连接
                    delayedResponseManager.closeTransportAndClearQueue(transport);
                }*/
                try {
                    transport.acknowledge(command, response, null);
                } catch (TransportException e) {
                    logger.warn("processing offset response error : ", e);
                }

            }
        };

        coordinator.handleQueryOffsets(offsetRequestMap, queryCallback);

        return null;
    }

    @Override
    public int[] type() {
        return new int[]{KafkaCommandKeys.LIST_OFFSETS};
    }
}
