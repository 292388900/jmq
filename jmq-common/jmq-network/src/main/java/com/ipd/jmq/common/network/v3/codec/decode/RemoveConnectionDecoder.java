package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.RemoveConnection;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.session.ConnectionId;
import io.netty.buffer.ByteBuf;

/**
 * 移除连接解码器
 */
public class RemoveConnectionDecoder implements PayloadDecoder<RemoveConnection> {

    @Override
    public RemoveConnection decode(final RemoveConnection payload, final ByteBuf in) throws Exception {
        if (in == null) {
            return payload;
        }
        // 1字节连接ID长度
        payload.setConnectionId(new ConnectionId(Serializer.readString(in)));

        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.REMOVE_CONNECTION;
    }
}