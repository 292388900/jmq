package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.RemoveProducer;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 移除生产者编码器
 */
public class RemoveProducerEncoder implements PayloadEncoder<RemoveProducer> {

    @Override
    public void encode(RemoveProducer payload, ByteBuf out) throws Exception {
        // 校验
        payload.validate();
        // 1字节生产者ID长度
        Serializer.write(payload.getProducerId().getProducerId(), out);
    }

    @Override
    public int type() {
        return 0;
    }
}