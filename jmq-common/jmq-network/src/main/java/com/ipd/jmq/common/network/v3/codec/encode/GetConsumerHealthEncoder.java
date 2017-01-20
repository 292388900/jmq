package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetConsumerHealth;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 消费者健康检测编码器
 */
public class GetConsumerHealthEncoder extends GetHealthEncoder implements PayloadEncoder<GetConsumerHealth> {

    @Override
    public void encode(final GetConsumerHealth payload, final ByteBuf out) throws Exception {
        super.encode(payload, out);
        Serializer.write(payload.getConsumerId(), out);
    }

    @Override
    public int type() {
        return CmdTypes.GET_CONSUMER_HEALTH;
    }
}