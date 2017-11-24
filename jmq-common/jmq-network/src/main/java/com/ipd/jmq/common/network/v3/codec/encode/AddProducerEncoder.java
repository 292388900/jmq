package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.AddProducer;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 添加生产者编码器
 */
public class AddProducerEncoder implements PayloadEncoder<AddProducer> {

    @Override
    public void encode(final AddProducer payload, final ByteBuf out) throws Exception {
        // 1字节生产者ID长度
        Serializer.write(payload.getProducerId().getProducerId(), out);
        // 1字节主题长度
        Serializer.write(payload.getTopic(), out);
    }

    @Override
    public int type() {
        return CmdTypes.ADD_PRODUCER;
    }
}