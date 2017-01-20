package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Identity;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.toolkit.network.Ipv4;
import io.netty.buffer.ByteBuf;

/**
 * 认证解码器
 */
public class IdentityDecoder implements PayloadDecoder<Identity> {

    @Override
    public Identity decode(final Identity payload, final ByteBuf in) throws Exception {
        // 6字节地址
        StringBuilder sb = new StringBuilder();
        Ipv4.toAddress(Serializer.readBytes(in, 6), sb);

        String address = sb.toString();

        Broker broker = new Broker(address.replaceAll("[.:]", "_"));
        broker.setAlias(Serializer.readString(in));
        broker.setDataCenter(in.readByte());
        payload.setBroker(broker);
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.IDENTITY;
    }
}