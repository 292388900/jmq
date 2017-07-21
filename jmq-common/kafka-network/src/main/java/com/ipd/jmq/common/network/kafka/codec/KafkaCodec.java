package com.ipd.jmq.common.network.kafka.codec;

import com.ipd.jmq.common.network.kafka.codec.decode.KafkaDecoder;
import com.ipd.jmq.common.network.kafka.codec.encode.*;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandFactory;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaHeader;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.exception.KafkaException;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.common.network.v3.command.CommandFactory;
import com.ipd.jmq.common.network.v3.command.Direction;
import com.ipd.jmq.common.network.v3.codec.Codec;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka编解码器
 *
 * @author lindeqiang, luoruiheng
 * @since 2016/8/4 10:36
 */
public class KafkaCodec implements Codec {
    private static final Logger logger = LoggerFactory.getLogger(KafkaCodec.class);
    private int maxSize = 1024 * 1024 * 4 + 1024;

    private KafkaDecoder decoder;
    private KafkaEncoder encoder;
    private CommandFactory commandFactory = new KafkaCommandFactory();

    public KafkaCodec() {
        this.decoder = new KafkaDecoder();
        this.encoder = new KafkaEncoder();
    }

    public KafkaCodec(int frameMaxSize) {
        if (frameMaxSize > 0) {
            this.maxSize = frameMaxSize;
        }
        this.decoder = new KafkaDecoder();
        this.encoder = new KafkaEncoder();
    }

    @Override
    public Object decode(Class clazz, ByteBuf in) throws TransportException.CodecException {
        if (in == null) {
            return null;
        }
        short commandKeyInKafka = 0;
        try {

            int size = in.readInt();
            if (size <= 0) {
                throw new KafkaException(String.format("%d is not a valid request size", size));
            }
            if (size > maxSize) {
                throw new KafkaException(String.format("Request of length %d is not valid, it is larger than the maximum size of %d bytes", size, maxSize));
            }

            short commandKey = in.readShort();
            commandKeyInKafka = commandKey;
            if (commandKey < KafkaCommandKeys.BASE) {
                commandKey += KafkaCommandKeys.BASE;
            }

            //boolean customRequest = commandKey == KafkaCommandKeys.OFFSET_QUERY || commandKey == KafkaCommandKeys.UPDATE_TOPICS_BROKER;
            boolean customRequest = commandKey >= KafkaCommandKeys.UPDATE_TOPICS_BROKER;
            KafkaHeader kafkaHeader = null;
            Command command = null;

            if (customRequest) {
                // 自定义命令解析
                int direction = in.readInt();
                if (direction == Direction.REQUEST.getValue()) {
                    // 自定义命令
                    kafkaHeader = KafkaHeader.Builder.request(commandKey);
                } else if (direction == Direction.RESPONSE.getValue()) {
                    kafkaHeader = KafkaHeader.Builder.customResponse(commandKey);
                    if (null == kafkaHeader) {
                        throw new KafkaException("response decode error");
                    }
                } else {
                    throw new KafkaException("direction decode error");
                }
            } else {
                kafkaHeader = KafkaHeader.Builder.request(commandKey);
            }

            kafkaHeader.setCommandKey(commandKey);
            command = commandFactory.create(kafkaHeader);

            // 反序列化payload
            KafkaRequestOrResponse payload = (KafkaRequestOrResponse)command.getPayload();
            if (payload != null) {
                payload = decoder.decode(payload, in);
                if (payload == null) {
                    throw new KafkaException("decode request error");
                }
            } else {
                // see http://kafka.apache.org/protocol.html#protocol_api_keys
                if (commandKeyInKafka > 33) {
                    throw new KafkaException("not supported command type");
                }
                // TODO: others vaild api-key are not implemented now, just return null
                return null;
            }
            return command;
        } catch (Exception e) {
            logger.error("not support commandKeyInKafka={}, " + e.getMessage(), commandKeyInKafka, e);
            throw new TransportException.CodecException(e.getMessage());
        }
    }

    @Override
    public void encode(Object o, ByteBuf out) throws TransportException.CodecException {
        if (out == null) {
            return;
        }
        if (o instanceof Command) {
            //类型转换
            Command command = (Command) o;
            //payload序列化
            KafkaRequestOrResponse payload = (KafkaRequestOrResponse)command.getPayload();
            if (payload != null) {
                encoder.encode(payload, out);
            }
        } else {
            throw new TransportException.CodecException(String.format("Command type %s not supported", o.getClass()));
        }
    }
}
