package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetProducerHealth;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 发送者健康检测解码器
 */
public class GetProducerHealthDecoder extends GetHealthDecoder implements PayloadDecoder<GetProducerHealth> {

    @Override
    public GetProducerHealth decode(final GetProducerHealth payload, final ByteBuf in) throws Exception {
        super.decode(payload, in);
        payload.producerId(Serializer.readString(in));
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.GET_PRODUCER_HEALTH;
    }
}