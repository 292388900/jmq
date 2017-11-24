package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.AddConsumer;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.session.ConsumerId;
import io.netty.buffer.ByteBuf;

/**
 * 添加消费者解码器
 */
public class AddConsumerDecoder implements PayloadDecoder<AddConsumer> {

    @Override
    public AddConsumer decode(final AddConsumer payload, final ByteBuf in) throws Exception {
        // 1字节消费者ID长度
        payload.setConsumerId(new ConsumerId(Serializer.readString(in)));
        // 1字节主题长度
        payload.setTopic(Serializer.readString(in));
        // 2字节选择器长度
        payload.setSelector(Serializer.readString(in, 2));

        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.ADD_CONSUMER;
    }
}