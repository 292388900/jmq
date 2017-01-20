package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.ClientProfileAck;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import io.netty.buffer.ByteBuf;

/**
 * 客户端性能应答编码器
 */
public class ClientProfileAckEncoder implements PayloadEncoder<ClientProfileAck> {

    @Override
    public void encode(ClientProfileAck payload, ByteBuf out) throws Exception {
        payload.validate();
        out.writeInt(payload.getInterval());
        out.writeInt(payload.getMachineMetricsInterval());
        out.writeByte(payload.getOpener());
    }

    @Override
    public int type() {
        return CmdTypes.CLIENT_PROFILE_ACK;
    }
}