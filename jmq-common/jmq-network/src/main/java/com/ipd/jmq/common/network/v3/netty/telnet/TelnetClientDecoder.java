package com.ipd.jmq.common.network.v3.netty.telnet;

import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.common.network.protocol.telnet.TelnetEscape;
import com.ipd.jmq.common.network.protocol.telnet.TelnetHeader;
import com.ipd.jmq.common.network.protocol.telnet.TelnetResponse;
import com.ipd.jmq.toolkit.lang.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * Created by zhangkepeng on 16-11-24.
 */
public class TelnetClientDecoder extends ByteToMessageDecoder {

    public TelnetClientDecoder() {
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) throws Exception {
        int length = in.readableBytes();
        if (length == 0) {
            return;
        }

        // 读取缓冲区数据
        byte message[] = new byte[length];
        in.readBytes(message);
        String string = new String(message, Charsets.UTF_8);
        // 取TelnetEscape.CR前的字符串
        String jsonResult = string.substring(0, string.indexOf(TelnetEscape.CR));
        out.add(new Command(TelnetHeader.Builder.response(), new TelnetResponse(jsonResult)));
    }
}
