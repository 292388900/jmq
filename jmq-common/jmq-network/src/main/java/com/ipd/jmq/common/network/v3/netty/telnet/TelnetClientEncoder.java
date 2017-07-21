package com.ipd.jmq.common.network.v3.netty.telnet;

import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.common.network.v3.codec.Encoder;
import com.ipd.jmq.common.network.v3.protocol.telnet.TelnetEscape;
import com.ipd.jmq.common.network.v3.protocol.telnet.TelnetRequest;
import com.ipd.jmq.toolkit.lang.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by zhangkepeng on 16-11-24.
 *
 * 兼容控制台命令， 去掉对prompt编码
 */
public class TelnetClientEncoder extends MessageToByteEncoder<Command> {

    protected Encoder encoder;

    public TelnetClientEncoder() {
        super();
    }

    public TelnetClientEncoder(Encoder encoder) {
        super();
        this.encoder = encoder;
    }

    @Override
    protected void encode(final ChannelHandlerContext ctx, final Command command, final ByteBuf out) throws Exception {
        Object payload = command.getPayload();

        if (payload != null) {
            String input;
            if (payload instanceof TelnetRequest) {
                TelnetRequest request = (TelnetRequest) payload;
                input = request.getInput();
                if (input != null && !input.isEmpty()) {
                    out.writeBytes((input.getBytes(Charsets.UTF_8)));
                }

                if (input == null || !input.endsWith(TelnetEscape.CR)) {
                    out.writeBytes(TelnetEscape.CR.getBytes(Charsets.UTF_8));
                }
            } else if (payload instanceof String) {
                input = (String) payload;
                out.writeBytes(input.getBytes(Charsets.UTF_8));
                if (input == null || !input.endsWith(TelnetEscape.CR)) {
                    out.writeBytes(TelnetEscape.CR.getBytes(Charsets.UTF_8));
                }
            } else if (encoder != null) {
                encoder.encode(payload, out);
                out.writeBytes(TelnetEscape.CR.getBytes(Charsets.UTF_8));
            } else {
                input = payload.toString();
                out.writeBytes(input.getBytes(Charsets.UTF_8));
                if (input == null || !input.endsWith(TelnetEscape.CR)) {
                    out.writeBytes(TelnetEscape.CR.getBytes(Charsets.UTF_8));
                }
            }
        } else {
            out.writeBytes(TelnetEscape.CR.getBytes(Charsets.UTF_8));
        }
    }
}
