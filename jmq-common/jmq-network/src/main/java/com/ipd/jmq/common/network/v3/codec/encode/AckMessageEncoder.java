package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.message.MessageLocation;
import com.ipd.jmq.common.network.v3.command.AckMessage;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.TransportException;
import io.netty.buffer.ByteBuf;

public class AckMessageEncoder implements PayloadEncoder<AckMessage> {
    /**
     * 编码器
     *
     * @param payload
     * @param out
     * @throws TransportException.CodecException
     */
    public void encode(final AckMessage payload, final ByteBuf out) throws Exception {
        //1字节确认消息来源
        out.writeByte(payload.getSource());
        // 1字节消费者ID长度
        Serializer.write(payload.getConsumerId().getConsumerId(), out, 1);
        MessageLocation[] locations = payload.getLocations();
        String topic = locations[0].getTopic();
        byte[] address = locations[0].getAddress();

        // 6字节服务地址
        out.writeBytes(address);
        // 1字节主题长度
        Serializer.write(topic, out, 1);
        // 2字节条数
        out.writeShort(locations.length);

        for (MessageLocation location : locations) {
            //1字节队列ID
            out.writeByte(location.getQueueId());
            // 8字节队列偏移
            out.writeLong(location.getQueueOffset());
            // 8自己日志偏移
            out.writeLong(location.getJournalOffset());
        }
    }

    @Override
    public int type() {
        return CmdTypes.ACK_MESSAGE;
    }
}