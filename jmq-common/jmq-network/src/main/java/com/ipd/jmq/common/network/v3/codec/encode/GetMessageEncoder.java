package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetMessage;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 获取消息编码器
 */
public class GetMessageEncoder implements PayloadEncoder<GetMessage> {

    @Override
    public void encode(final GetMessage payload, final ByteBuf out) throws Exception {
        payload.validate();
        // 1字节消费者ID长度
        Serializer.write(payload.getConsumerId().getConsumerId(), out);
        // 1字节主题长度
        Serializer.write(payload.getTopic(), out);
        // 2字节数量
        out.writeShort(payload.getCount());
        // 4字节长轮询超时
        out.writeInt(payload.getLongPull());
        // 2字节队列号
        out.writeShort(payload.getQueueId());
        // 8字节队列偏移量
        out.writeLong(payload.getOffset());
    }

    @Override
    public int type() {
        return CmdTypes.GET_MESSAGE;
    }
}