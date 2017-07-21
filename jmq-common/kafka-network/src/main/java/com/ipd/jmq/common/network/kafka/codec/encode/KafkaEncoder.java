package com.ipd.jmq.common.network.kafka.codec.encode;

import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.TransportException;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by zhangkepeng on 16-8-15.
 */
public class KafkaEncoder {

    //命令实体编码器
    private static ConcurrentMap<Integer, PayloadEncoder> payloadEncoders = new ConcurrentHashMap<Integer, PayloadEncoder>();


    /**
     * kafka payload编码器
     *
     * @param payload
     * @param out
     * @throws TransportException.CodecException
     */
    public void encode(final KafkaRequestOrResponse payload, final ByteBuf out) throws TransportException.CodecException {
        if (payload == null || out == null) {
            return;
        }

        try {
            PayloadEncoder encoder = payloadEncoders.get(payload.type());
            if (encoder != null) {
                encoder.encode(payload, out);
            } else {
                throw new IllegalStateException("Encoder unimplemented");
            }
        } catch (Exception e) {
            throw new TransportException.CodecException(e.getMessage());
        }
    }

    /**
     * 注入实体编码器
     *
     * @param encoder 实体编码器
     * @return 是否注入成功
     */
    public static boolean register(PayloadEncoder encoder) {
        int type = (int) encoder.type();
        return payloadEncoders.putIfAbsent(type, encoder) == null;
    }

    static {
        register(new FetchEncoder());
        register(new OffsetCommitEncoder());
        register(new OffsetEncoder());
        register(new OffsetFetchEncoder());
        register(new OffsetQueryEncoder());
        register(new ProduceEncoder());
        register(new TopicMetadataEncoder());
        register(new GroupCoordinatorEncoder());
        register(new UpdateTopicsBrokerEncoder());
        register(new JoinGroupEncoder());
        register(new HeartbeatEncoder());
        register(new LeaveGroupEncoder());
        register(new SyncGroupEncoder());
    }
}
