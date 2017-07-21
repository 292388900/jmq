package com.ipd.jmq.common.network.v3.netty.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * 分配缓冲区，命令可以实现该接口自定义缓冲区，实现零拷贝
 */
public interface BufAllocator {

    /**
     * 分配内存
     *
     * @param ctx          上下文
     * @param preferDirect 优先系统内存标示
     * @return 缓冲区
     * @throws Exception
     */
    ByteBuf allocate(ChannelHandlerContext ctx, boolean preferDirect) throws Exception;

}
