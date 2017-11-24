package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetConsumerHealth;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 消费者健康检测解码器
 */
public class GetConsumerHealthDecoder extends GetHealthDecoder implements PayloadDecoder<GetConsumerHealth> {

    @Override
    public GetConsumerHealth decode(final GetConsumerHealth payload, final ByteBuf in) throws Exception {
        super.decode(payload, in);
        payload.setConsumerId(Serializer.readString(in));
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.GET_CONSUMER_HEALTH;
    }
}