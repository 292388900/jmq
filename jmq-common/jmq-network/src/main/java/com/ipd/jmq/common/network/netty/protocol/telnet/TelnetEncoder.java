package com.ipd.jmq.common.network.netty.protocol.telnet;

import com.ipd.jmq.common.network.netty.codec.Encoder;
import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.common.network.protocol.telnet.TelnetEscape;
import com.ipd.jmq.common.network.protocol.telnet.TelnetResponse;
import com.ipd.jmq.toolkit.lang.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Telnent编码器
 */
public class TelnetEncoder extends MessageToByteEncoder<Command> {

    protected Encoder encoder;
    protected String prompt = ">";

    public TelnetEncoder() {
        super();
    }

    public TelnetEncoder(String prompt, Encoder encoder) {
        super();
        this.prompt = prompt;
        this.encoder = encoder;
    }

    @Override
    protected void encode(final ChannelHandlerContext ctx, final Command command, final ByteBuf out) throws Exception {
        Object payload = command.getPayload();
        if (payload != null) {
            String message;
            if (payload instanceof TelnetResponse) {
                TelnetResponse response = (TelnetResponse) payload;
                message = response.getMessage();
                if (message != null && !message.isEmpty()) {
                    out.writeBytes((message.getBytes(Charsets.UTF_8)));
                }
                if (response.isNewLine()) {
                    if (message == null || !message.endsWith(TelnetEscape.CR)) {
                        out.writeBytes(TelnetEscape.CR.getBytes(Charsets.UTF_8));
                    }
                }
                if (response.isPrompt()) {
                    out.writeBytes(prompt.getBytes(Charsets.UTF_8));
                }
            } else if (payload instanceof String) {
                message = (String) payload;
                out.writeBytes(message.getBytes(Charsets.UTF_8));
                if (message == null || !message.endsWith(TelnetEscape.CR)) {
                    out.writeBytes(TelnetEscape.CR.getBytes(Charsets.UTF_8));
                }
                out.writeBytes(prompt.getBytes(Charsets.UTF_8));
            } else if (encoder != null) {
                encoder.encode(payload, out);
                out.writeBytes(TelnetEscape.CR.getBytes(Charsets.UTF_8)).writeBytes(prompt.getBytes(Charsets.UTF_8));
            } else {
                message = payload.toString();
                out.writeBytes(message.getBytes(Charsets.UTF_8));
                if (message == null || !message.endsWith(TelnetEscape.CR)) {
                    out.writeBytes(TelnetEscape.CR.getBytes(Charsets.UTF_8));
                }
                out.writeBytes(prompt.getBytes(Charsets.UTF_8));
            }
        } else {
            out.writeBytes(TelnetEscape.CR.getBytes(Charsets.UTF_8)).writeBytes(prompt.getBytes(Charsets.UTF_8));
        }
    }
}
