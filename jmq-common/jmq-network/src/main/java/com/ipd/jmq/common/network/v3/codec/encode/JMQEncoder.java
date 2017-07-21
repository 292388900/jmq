package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.JMQHeader;
import com.ipd.jmq.common.network.v3.command.JMQPayload;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.v3.command.Direction;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * JMQ编码器.
 *
 * @author lindeqiang
 * @since 2016/8/4 15:45
 */
public class JMQEncoder {
    //命令实体编码器
    private static ConcurrentMap<Integer, PayloadEncoder> payloadEncoders = new ConcurrentHashMap<Integer, PayloadEncoder>();

    /**
     * JMQ  命令头序列化
     *
     * @param header 命令头
     * @param out    输出流
     * @throws TransportException.CodecException
     */
    public void encode(final JMQHeader header, final ByteBuf out) throws TransportException.CodecException {
        if (out == null || header == null) {
            return;
        }

        try {
            // 1 字节版本
            out.writeByte(header.getVersion());
            // 1个字节标识字段
            int identity = (byte) ((header.getDirection().ordinal() & 0x1) | ((header.getAcknowledge().ordinal()) << 1 & 0x6));
            out.writeByte(identity);

            // 4个字节请求ID
            out.writeInt(header.getRequestId());
            // 1个字节命令类型
            out.writeByte(header.getType());
            // 8个字节发送时间
            out.writeLong(header.getTime());

            // 如果为响应类型，需写入响应状态吗和错误消息
            if (header.getDirection() == Direction.RESPONSE) {
                // 1个字节的状态码
                out.writeByte(header.getStatus());
                // 2个字节的异常长度
                // 异常信息
                Serializer.write(header.getError(), out, 2);
            }
        } catch (Exception e) {
            throw new TransportException.CodecException(e.getMessage());
        }

    }

    /**
     * JMQ 命令实体序列化
     *
     * @param payload 命令实体
     * @param out     输出流
     * @throws TransportException.CodecException
     */
    public void encode(final JMQPayload payload, final ByteBuf out) throws TransportException.CodecException {
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
        return payloadEncoders.putIfAbsent(encoder.type(), encoder) == null;
    }

    static {
        register(new AckMessageEncoder());
        register(new GetMessageEncoder());
        register(new GetMessageAckEncoder());
        register(new RetryMessageEncoder());
        register(new PutRetryEncoder());
        register(new GetRetryCountEncoder());
        register(new GetRetryCountAckEncoder());
        register(new UpdateRetryEncoder());
        register(new GetRetryEncoder());
        register(new GetRetryAckEncoder());
        register(new TxPrepareEncoder());
        register(new TxPrepareAckEncoder());
        register(new TxCommitEncoder());
        register(new TxRollbackEncoder());
        register(new TxFeedbackEncoder());
        register(new TxFeedbackAckEncoder());
        register(new PutMessageEncoder());
        register(new GetClusterEncoder());
        register(new GetClusterAckEncoder());
        register(new GetProducerHealthEncoder());
        register(new GetConsumerHealthEncoder());
        register(new AddConnectionEncoder());
        register(new RemoveConnectionEncoder());
        register(new AddProducerEncoder());
        register(new RemoveProducerEncoder());
        register(new AddConsumerEncoder());
        register(new RemoveConsumerEncoder());
        register(new ClientProfileEncoder());
        register(new ClientProfileAckEncoder());
        register(new IdentityEncoder());
        register(new GetOffsetEncoder());
        register(new GetOffsetAckEncoder());
        register(new GetJournalEncoder());
        register(new GetJournalAckEncoder());
        register(new GetConsumeOffsetEncoder());
        register(new GetConsumeOffsetAckEncoder());
        register(new SystemCmdEncoder());
        register(new ResetAckOffsetEncoder());
    }

}
