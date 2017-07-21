package com.ipd.jmq.common.network.kafka.codec.decode;

import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by zhangkepeng on 16-8-15.
 */
public class KafkaDecoder {

    private static final Logger logger = LoggerFactory.getLogger(KafkaDecoder.class);

    private static ConcurrentMap<Integer, PayloadDecoder> payloadDecoders = new ConcurrentHashMap<Integer, PayloadDecoder>();

    /**
     * 命令实体反序列化
     *
     * @param payload 命令实体
     * @param in      输入数据
     * @return
     * @throws TransportException.CodecException
     */
    public KafkaRequestOrResponse decode(final KafkaRequestOrResponse payload, final ByteBuf in) throws TransportException.CodecException {
        if (payload == null || in == null) {
            return null;
        }

        try {
            PayloadDecoder decoder = payloadDecoders.get(payload.type());
            if (decoder != null) {
                return (KafkaRequestOrResponse) decoder.decode(payload, in);
            } else {
                throw new IllegalStateException("Decoder unimplemented!");
            }
        } catch (Exception e) {
            throw new TransportException.CodecException(e.getMessage());
        }
    }


    /**
     * 注册解码器
     *
     * @param decoder 解码器
     * @return 是否注入成功
     */
    public static boolean register(PayloadDecoder decoder) {
        if (decoder == null) {
            logger.error("register decoder can't be null");
            return false;
        }
        int type = (int) decoder.type();
        return payloadDecoders.putIfAbsent(type, decoder) == null;
    }

    static {
        register(new FetchDecoder());
        register(new OffsetCommitDecoder());
        register(new OffsetDecoder());
        register(new OffsetFetchDecoder());
        register(new OffsetQueryDecoder());
        register(new ProduceDecoder());
        register(new TopicMetadataDecoder());
        register(new GroupCoordinatorDecoder());
        register(new UpdateTopicsBrokerDecoder());
        register(new JoinGroupDecoder());
        register(new HeartbeatDecoder());
        register(new LeaveGroupDecoder());
        register(new SyncGroupDecoder());
    }
}
