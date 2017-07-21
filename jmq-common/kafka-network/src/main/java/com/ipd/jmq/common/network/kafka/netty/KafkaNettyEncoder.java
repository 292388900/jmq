package com.ipd.jmq.common.network.kafka.netty;

import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.common.network.v3.codec.Encoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by zhangkepeng on 16-7-26.
 */
public class KafkaNettyEncoder extends MessageToByteEncoder<Command> {

    private Encoder encoder;

    public KafkaNettyEncoder(Encoder encoder) {
        super();
        this.encoder = encoder;
    }

    @Override
    protected void encode(final ChannelHandlerContext ctx, final Command command, final ByteBuf out) throws Exception {
        encoder.encode(command, out);
    }
}
