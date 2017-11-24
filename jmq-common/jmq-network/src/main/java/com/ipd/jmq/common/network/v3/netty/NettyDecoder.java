package com.ipd.jmq.common.network.v3.netty;

import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.common.network.v3.codec.Codec;
import com.ipd.jmq.common.network.v3.codec.Decoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * JMQ解析器.
 *
 * @author lindeqiang
 * @since 2016/7/20 11:19
 */
public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    public static final int FRAME_MAX_SIZE = 1024 * 1024 * 4 + 1024;
    private Decoder codec;

    public NettyDecoder(Codec codec) {
        super(FRAME_MAX_SIZE, 2, 4, 0, 6);
        this.codec = codec;
    }


    public NettyDecoder(Codec codec, int frameSize) {
        super(frameSize, 2, 4, 0, 6);
        this.codec = codec;
    }

    public NettyDecoder(Codec codec, int maxFrameLength, int lengthFieldOffset, int lengthFieldLength) {
        super(maxFrameLength, lengthFieldOffset, lengthFieldLength);
        this.codec = codec;
    }

    public NettyDecoder(Codec codec, int maxFrameLength, int lengthFieldOffset, int lengthFieldLength, int
            lengthAdjustment,
                        int initialBytesToStrip) {
        super(maxFrameLength, lengthFieldOffset, lengthFieldLength, lengthAdjustment, initialBytesToStrip);
        this.codec = codec;
    }

    @Override
    protected Object decode(final ChannelHandlerContext ctx, final ByteBuf in) throws Exception {
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) {
            return null;
        }
        //构造请求命令
        return codec.decode(Command.class, frame);
    }

    @Override
    protected ByteBuf extractFrame(final ChannelHandlerContext ctx, final ByteBuf buffer, final int index,
                                   final int length) {
        return buffer.slice(index, length);
    }
}
