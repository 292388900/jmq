package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.Language;
import com.ipd.jmq.common.network.v3.command.AddConnection;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.session.ConnectionId;
import io.netty.buffer.ByteBuf;

/**
 * 添加链接解码器
 */
public class AddConnectionDecoder implements PayloadDecoder<AddConnection> {

    @Override
    public AddConnection decode(final AddConnection payload, final ByteBuf in) throws Exception {
        if (in == null) {
            return null;
        }
        // 1字节链接ID长度
        payload.setConnectionId(new ConnectionId(Serializer.readString(in)));
        // 1字节用户长度
        payload.setUser(Serializer.readString(in));
        // 1字节密码长度
        payload.setPassword(Serializer.readString(in));
        // 1字节应用长度
        payload.setApp(Serializer.readString(in));
        // 1字节语言
        payload.setLanguage(Language.valueOf(in.readUnsignedByte()));
        // 1字节版本长度
        payload.setClientVersion(Serializer.readString(in));

        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.ADD_CONNECTION;
    }
}