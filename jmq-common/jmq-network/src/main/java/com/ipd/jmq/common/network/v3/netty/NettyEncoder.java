package com.ipd.jmq.common.network.v3.netty;

import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.common.network.netty.codec.Codec;
import com.ipd.jmq.common.network.netty.codec.Encoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * JMQ编码器.
 *
 * @author lindeqiang
 * @since 2016/7/20 11:21
 */
public class NettyEncoder extends MessageToByteEncoder<Command> {
    protected Encoder codec;

    public NettyEncoder(Codec codec) {
        this.codec = codec;
    }

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Command command, ByteBuf out) throws Exception {
        if (out == null || command == null || useAllocator(command)) {
            return;
        }

        codec.encode(command, out);
    }

    @Override
    protected ByteBuf allocateBuffer(final ChannelHandlerContext ctx, final Command command, final boolean
            preferDirect) throws Exception {

        if (useAllocator(command)) {
            // 自定义缓冲区实现
            return ((BufAllocator) command.getPayload()).allocate(ctx, preferDirect, command.getHeader());
        }
        return super.allocateBuffer(ctx, command, preferDirect);
    }

    boolean useAllocator(final Command command) {
        return command.getPayload() != null && command.getPayload() instanceof BufAllocator;
    }
}
