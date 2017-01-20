package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.message.MessageLocation;
import com.ipd.jmq.common.network.v3.command.AckMessage;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.session.ConsumerId;
import com.ipd.jmq.common.network.TransportException;
import io.netty.buffer.ByteBuf;

/**
 * 应答消息解码器
 */
public class AckMessageDecoder implements PayloadDecoder<AckMessage> {
    /**
     * @param payload
     * @param in
     * @return
     * @throws TransportException.CodecException
     */
    public AckMessage decode(final AckMessage payload, final ByteBuf in) throws Exception {
        payload.setSource(in.readByte());
        payload.setConsumerId(new ConsumerId(Serializer.readString(in)));
        // 6字节服务地址
        byte[] address = Serializer.readBytes(in, 6);
        // 1字节主题长度
        String topic = Serializer.readString(in);
        // 2字节条数
        int length = in.readUnsignedShort();
        MessageLocation[] locations = new MessageLocation[length];
        short queueId;
        long queueOffset;
        long journalOffset;
        for (int i = 0; i < length; i++) {
            // 1字节队列ID
            queueId = in.readUnsignedByte();
            // 8字节队列偏移
            queueOffset = in.readLong();
            // 8字节日志偏移
            journalOffset = in.readLong();

            locations[i] = new MessageLocation(address, topic, queueId, queueOffset, journalOffset);
        }

        payload.setLocations(locations);
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.ACK_MESSAGE;
    }
}