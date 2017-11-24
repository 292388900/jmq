package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.RemoveProducer;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.session.ProducerId;
import io.netty.buffer.ByteBuf;

/**
 * 移除生产者解码器
 */
public class RemoveProducerDecoder implements PayloadDecoder<RemoveProducer> {

    @Override
    public RemoveProducer decode(final RemoveProducer payload, final ByteBuf in) throws Exception {
        // 1字节生产者ID长度
        payload.setProducerId(new ProducerId(Serializer.readString(in)));

        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.REMOVE_PRODUCER;
    }
}