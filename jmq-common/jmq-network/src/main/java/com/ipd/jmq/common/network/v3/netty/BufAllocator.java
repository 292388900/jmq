package com.ipd.jmq.common.network.v3.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author lindeqiang
 * @since 2016/8/14 22:31
 */
public interface BufAllocator {
    ByteBuf allocate(ChannelHandlerContext ctx, boolean preferDirect, Object attach) throws Exception;
}
