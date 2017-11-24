package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.command.SystemCmd;
import io.netty.buffer.ByteBuf;

/**
 * 系统命令编码器
 */
public class SystemCmdEncoder implements PayloadEncoder<SystemCmd> {

    @Override
    public void encode(SystemCmd payload, ByteBuf out) throws Exception {
        payload.validate();
        Serializer.write(payload.getCmd(), out, 1, false);
        Serializer.write(payload.getUrl(), out, 2, false);
        out.writeInt(payload.getTimeout());
    }

    @Override
    public int type() {
        return CmdTypes.SYSTEM_COMMAND;
    }
}