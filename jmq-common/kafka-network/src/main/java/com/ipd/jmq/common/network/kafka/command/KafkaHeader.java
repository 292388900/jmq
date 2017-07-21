package com.ipd.jmq.common.network.kafka.command;

import com.ipd.jmq.common.model.Acknowledge;
import com.ipd.jmq.common.network.v3.command.Direction;
import com.ipd.jmq.common.network.v3.command.Header;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhangkepeng on 16-7-26.
 */
public class KafkaHeader implements Header {
    // 命令key
    protected short commandKey;
    // 请求ID
    protected int requestId;
    // 请求或应答
    protected Direction direction;
    // 应答方式
    protected Acknowledge acknowledge;

    private static final AtomicInteger REQUEST_ID = new AtomicInteger(0);
    // 只用于自定义命令 (CUSTOM_REQUEST_ID是为了防止和KAFKA原生命令的REQUEST_ID冲突)
    // map是为了多个自定义命令之间冲突
    private static ConcurrentMap<Short, AtomicInteger> CUSTOM_REQUEST_IDS = new ConcurrentHashMap<Short, AtomicInteger>();
    private static final AtomicInteger CUSTOM_REQUEST_ID = new AtomicInteger(-1);

    public short getCommandKey() {
        return commandKey;
    }

    public void setCommandKey(short commandKey) {
        this.commandKey = commandKey;
    }

    @Override
    public int getRequestId() {
        return requestId;
    }

    @Override
    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }

    @Override
    public Direction getDirection() {
        return direction;
    }

    @Override
    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    @Override
    public Acknowledge getAcknowledge() {
        return acknowledge;
    }

    @Override
    public void setAcknowledge(Acknowledge acknowledge) {
        this.acknowledge = acknowledge;
    }

    /**
     * 构造器
     */
    public static class Builder {
        KafkaHeader header = new KafkaHeader();

        public static Builder create() {
            return new Builder();
        }

        public static KafkaHeader response() {
            return create().direction(Direction.RESPONSE).acknowledge(Acknowledge.ACK_NO).build();
        }

        // 自定义命令 response
        public static KafkaHeader customResponse(final short kafkaCommandKeys) {
            AtomicInteger requestId = CUSTOM_REQUEST_IDS.get(kafkaCommandKeys);
            if (null != requestId) {
                KafkaHeader kafkaHeader = response(requestId.get());
                CUSTOM_REQUEST_IDS.remove(kafkaCommandKeys);
                return  kafkaHeader;
            }
            return null;
        }

        public static KafkaHeader response(int requestId) {
            return create().direction(Direction.RESPONSE).requestId(requestId).acknowledge(Acknowledge.ACK_NO).build();
        }

        /**
         *
         * @param kafkaCommandKeys kafka command
         * @return KafkaHeader
         */
        public static KafkaHeader request(final short kafkaCommandKeys) {
            return create().direction(Direction.REQUEST, kafkaCommandKeys).acknowledge(Acknowledge.ACK_RECEIVE).build();
        }

        public Builder requestId(int requestId) {
            header.setRequestId(requestId);
            return this;
        }

        public Builder direction(Direction direction) {
            header.setDirection(direction);
            if (header.direction == Direction.REQUEST) {
                header.requestId = REQUEST_ID.incrementAndGet();
            }
            return this;
        }

        private Builder direction(Direction direction, final short commandKeys) {
            header.setDirection(direction);
            if (header.direction == Direction.REQUEST) {
                if (commandKeys >= KafkaCommandKeys.UPDATE_TOPICS_BROKER) {
                    AtomicInteger old = CUSTOM_REQUEST_IDS.get(commandKeys);
                    if (null == old) {
                        old = CUSTOM_REQUEST_IDS.putIfAbsent(commandKeys, new AtomicInteger(CUSTOM_REQUEST_ID.incrementAndGet()));
                        if (null == old) {
                            header.requestId = CUSTOM_REQUEST_IDS.get(commandKeys).get();
                        }
                    } else {
                        header.requestId = CUSTOM_REQUEST_IDS.get(commandKeys).get();
                    }
                } else {
                    header.requestId = REQUEST_ID.incrementAndGet();
                }
            }
            return this;
        }

        public Builder acknowledge(Acknowledge acknowledge) {
            header.setAcknowledge(acknowledge);
            return this;
        }

        public KafkaHeader build() {
            return header;
        }

    }
}
