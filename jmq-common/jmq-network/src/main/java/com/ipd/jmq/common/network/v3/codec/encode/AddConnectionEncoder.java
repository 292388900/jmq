package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.AddConnection;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 添加链接编码器
 */
public class AddConnectionEncoder implements PayloadEncoder<AddConnection> {

    @Override
    public void encode(AddConnection payload, ByteBuf out) throws Exception {
        //字段校验
        payload.validate();
        // 1字节链接ID长度
        Serializer.write(payload.getConnectionId().getConnectionId(), out);
        // 1字节用户长度
        Serializer.write(payload.getUser(), out);
        // 1字节密码长度
        Serializer.write(payload.getPassword(), out);
        // 1字节应用长度
        Serializer.write(payload.getApp(), out);
        // 1字节语言
        out.writeByte(payload.getLanguage().ordinal());
        // clientVersion
        Serializer.write(payload.getClientVersion(), out);
    }

    @Override
    public int type() {
        return CmdTypes.ADD_CONNECTION;
    }
}