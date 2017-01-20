package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.RemoveConnection;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 移除连接编码器
 */
public class RemoveConnectionEncoder implements PayloadEncoder<RemoveConnection> {

    @Override
    public void encode(final RemoveConnection payload, final ByteBuf out) throws Exception {
        payload.validate();
        // 1字节连接ID长度
        Serializer.write(payload.getConnectionId().getConnectionId(), out);
    }

    @Override
    public int type() {
        return CmdTypes.REMOVE_CONNECTION;
    }
}