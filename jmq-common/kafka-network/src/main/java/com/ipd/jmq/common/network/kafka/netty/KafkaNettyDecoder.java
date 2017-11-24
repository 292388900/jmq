package com.ipd.jmq.common.network.kafka.netty;

import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.v3.codec.Decoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhangkepeng on 16-7-26.
 */
public class KafkaNettyDecoder extends LengthFieldBasedFrameDecoder {
    public static final int FRAME_MAX_SIZE = 1024 * 1024 * 4 + 1024;
    private final static Logger logger = LoggerFactory.getLogger(KafkaNettyDecoder.class);

    private Decoder decoder;


    public KafkaNettyDecoder(Decoder decoder) {
        super(FRAME_MAX_SIZE, 0, 4, 0, 0);
        this.decoder = decoder;
    }

    @Override
    protected Object decode(final ChannelHandlerContext ctx, final ByteBuf in) throws Exception {
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) {
            return null;
        }

        return decoder.decode(KafkaRequestOrResponse.class, frame);
    }

    @Override
    protected ByteBuf extractFrame(final ChannelHandlerContext ctx, final ByteBuf buffer, final int index,
                                   final int length) {
        return buffer.slice(index, length);
    }
}
