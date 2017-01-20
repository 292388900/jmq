package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetProducerHealth;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 发送者健康检测编码器
 */
public class GetProducerHealthEncoder extends GetHealthEncoder implements PayloadEncoder<GetProducerHealth> {

    @Override
    public void encode(final GetProducerHealth payload, final ByteBuf out) throws Exception {
        super.encode(payload, out);
        Serializer.write(payload.getProducerId(), out);
    }

    @Override
    public int type() {
        return CmdTypes.GET_PRODUCER_HEALTH;
    }
}