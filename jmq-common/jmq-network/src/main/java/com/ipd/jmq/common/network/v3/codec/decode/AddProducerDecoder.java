package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.AddProducer;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.session.ProducerId;
import io.netty.buffer.ByteBuf;

/**
 * 添加生产者解码器
 */
public class AddProducerDecoder implements PayloadDecoder<AddProducer> {

    @Override
    public AddProducer decode(final AddProducer payload,final ByteBuf in) throws Exception {
        // 1字节生产者ID长度
        payload.setProducerId(new ProducerId(Serializer.readString(in)));

        // 1字节主题长度
        payload.setTopic(Serializer.readString(in));
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.ADD_PRODUCER;
    }
}