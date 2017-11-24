package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.JMQHeader;
import com.ipd.jmq.common.network.v3.command.JMQPayload;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.model.Acknowledge;
import com.ipd.jmq.common.network.v3.command.Direction;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * JMQ解码器.
 *
 * @author lindeqiang
 * @since 2016/8/4 15:44
 */
public class JMQDecoder {
    private static ConcurrentMap<Integer, PayloadDecoder> payloadDecoders = new ConcurrentHashMap<Integer, PayloadDecoder>();

    /**
     * 命令头部反序列化
     *
     * @param header 头
     * @param in     输入数据
     * @return
     * @throws TransportException.CodecException
     */
    public JMQHeader decode(final JMQHeader header, ByteBuf in) throws TransportException.CodecException {
        try {
            // 1字节版本
            header.setVersion(in.readByte());
            // 1个字节标识字段 (其中1-1：请求应答标识; 2-3:应答类型; 4-5:事务类型; 6-8:其它)
            int flag = in.readUnsignedByte();
            header.setDirection(Direction.valueOf(flag & 0x1));
            // 应答类型
            header.setAcknowledge(Acknowledge.valueOf((flag >> 1) & 0x3));
            // 4个字节请求ID
            header.setRequestId(in.readInt());
            // 1个字节命令类型
            header.setType(in.readUnsignedByte());
            // 8个字节发送时间
            header.setTime(in.readLong());
            // 如果为响应类型，需读响应状态吗和错误消息
            if (header.getDirection() == Direction.RESPONSE) {
                // 1个字节的状态码
                header.setStatus(in.readUnsignedByte());
                // 2个字节的异常长度
                // 异常信息
                header.setError(Serializer.readString(in, 2));
            }
            return header;
        } catch (Exception e) {
            throw new TransportException.CodecException(e.getMessage());
        }
    }


    /**
     * 命令实体反序列化
     *
     * @param payload 命令实体
     * @param in      输入数据
     * @return
     * @throws TransportException.CodecException
     */
    public JMQPayload decode(final JMQPayload payload, final ByteBuf in) throws TransportException.CodecException {
        if (payload == null || in == null) {
            return null;
        }

        try {
            PayloadDecoder decoder = payloadDecoders.get(payload.type());
            if (decoder != null) {
                return decoder.decode(payload, in);
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
        return payloadDecoders.putIfAbsent(decoder.type(), decoder) == null;
    }

    static {
        register(new AckMessageDecoder());
        register(new GetMessageDecoder());
        register(new GetMessageAckDecoder());
        register(new RetryMessageDecoder());
        register(new PutRetryDecoder());
        register(new GetRetryCountDecoder());
        register(new GetRetryAckDecoder());
        register(new UpdateRetryDecoder());
        register(new GetRetryCountDecoder());
        register(new GetRetryCountAckDecoder());
        register(new TxPrepareDecoder());
        register(new TxPrepareAckDecoder());
        register(new TxCommitDecoder());
        register(new TxRollbackDecoder());
        register(new TxFeedbackDecoder());
        register(new TxFeedbackAckDecoder());
        register(new PutMessageDecoder());
        register(new GetClusterDecoder());
        register(new GetClusterAckDecoder());
        register(new GetProducerHealthDecoder());
        register(new GetConsumerHealthDecoder());
        register(new AddConnectionDecoder());
        register(new RemoveConnectionDecoder());
        register(new AddProducerDecoder());
        register(new RemoveProducerDecoder());
        register(new AddConsumerDecoder());
        register(new RemoveConsumerDecoder());
        register(new ClientProfileDecoder());
        register(new ClientProfileAckDecoder());
        register(new IdentityDecoder());
        register(new GetOffsetDecoder());
        register(new GetOffsetAckDecoder());
        register(new GetJournalDecoder());
        register(new GetJournalAckDecoder());
        register(new GetConsumeOffsetDecoder());
        register(new GetConsumeOffsetAckDecoder());
        register(new SystemCmdDecoder());

        register(new ResetAckOffsetDecoder());


    }
}