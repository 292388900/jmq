package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetMessage;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.session.ConsumerId;
import io.netty.buffer.ByteBuf;

/**
 * 获取消息解码器
 */
public class GetMessageDecoder implements PayloadDecoder<GetMessage> {

    @Override
    public GetMessage decode(final GetMessage payload, final ByteBuf in) throws Exception {
        // 1字节消费者ID长度
        payload.setConsumerId(new ConsumerId(Serializer.readString(in)));
        // 1字节主题长度
        payload.setTopic(Serializer.readString(in));
        // 2字节数量
        payload.setCount(in.readShort());
        // 4字节长轮询超时
        payload.setLongPull(in.readInt());
        // 队列和偏移量及扩展
        // 2字节队列号
        payload.setQueueId(in.readShort());
        // 8字节偏移量
        payload.setOffset(in.readLong());

        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.GET_MESSAGE;
    }


}