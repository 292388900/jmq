package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.ClientProfileAck;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import io.netty.buffer.ByteBuf;

/**
 * 客户端性能应答解码器
 */
public class ClientProfileAckDecoder implements PayloadDecoder<ClientProfileAck> {

    @Override
    public ClientProfileAck decode(final ClientProfileAck payload, final ByteBuf in) throws Exception {
        payload.setInterval(in.readInt());
        payload.setMachineMetricsInterval(in.readInt());
        payload.setOpener(in.readByte());
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.CLIENT_PROFILE_ACK;
    }
}