package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.command.SystemCmd;
import io.netty.buffer.ByteBuf;

/**
 * 系统命令解码器
 */
public class SystemCmdDecoder implements PayloadDecoder<SystemCmd> {

    @Override
    public SystemCmd decode(SystemCmd payload, ByteBuf in) throws Exception {
        payload.setCmd(Serializer.readString(in, 1));
        payload.setUrl(Serializer.readString(in, 2));
        payload.setTimeout(in.readInt());
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.SYSTEM_COMMAND;
    }
}