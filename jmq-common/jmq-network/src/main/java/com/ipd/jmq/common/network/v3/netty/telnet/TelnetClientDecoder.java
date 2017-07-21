package com.ipd.jmq.common.network.v3.netty.telnet;

import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.common.network.v3.protocol.telnet.TelnetEscape;
import com.ipd.jmq.common.network.v3.protocol.telnet.TelnetHeader;
import com.ipd.jmq.common.network.v3.protocol.telnet.TelnetResponse;
import com.ipd.jmq.toolkit.lang.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LineBasedFrameDecoder;

import java.util.List;

/**
 * Created by zhangkepeng on 16-11-24.
 */
public class TelnetClientDecoder extends LineBasedFrameDecoder {
    public TelnetClientDecoder(int maxLength, boolean stripDelimiter, boolean failFast) {
        super(maxLength, stripDelimiter, failFast);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
        ByteBuf lineBuf = (ByteBuf) super.decode(ctx, buffer);

        if (lineBuf == null || lineBuf.readableBytes() == 0) {
            return null;
        } else {
            // 读取缓冲区数据
            String string = lineBuf.toString(Charsets.UTF_8);
            if(string != null && string.startsWith(">"))
                string = string.substring(1);
            // 取TelnetEscape.CR前的字符串
            return new Command(TelnetHeader.Builder.response(), new TelnetResponse(string));
        }
    }
}
