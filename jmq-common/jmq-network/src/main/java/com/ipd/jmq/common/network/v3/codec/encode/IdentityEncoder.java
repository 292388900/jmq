package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Identity;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.toolkit.network.Ipv4;
import io.netty.buffer.ByteBuf;

/**
 * 认证编码器
 */
public class IdentityEncoder implements PayloadEncoder<Identity> {

    @Override
    public void encode(final Identity payload,final ByteBuf out) throws Exception {
        Broker broker = payload.getBroker();
        // 6字节地址
        String ip = broker.getName();
        out.writeBytes(Ipv4.toByte(ip));
        Serializer.write(broker.getAlias(), out);
        out.writeByte(broker.getDataCenter());
    }

    @Override
    public int type() {
        return CmdTypes.IDENTITY;
    }
}