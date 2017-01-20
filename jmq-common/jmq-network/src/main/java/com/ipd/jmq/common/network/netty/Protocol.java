package com.ipd.jmq.common.network.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;

/**
 * Created by hexiaofeng on 16-6-23.
 */
public interface Protocol {
    String PROTOCOL = "protocol";

    /**
     * 是否支持该数据头
     *
     * @param buf 字节数组
     * @return
     */
    boolean support(final ByteBuf buf);

    /**
     * 是否是长连接
     *
     * @return 长连接标示
     */
    boolean keeplive();

    /**
     * 构建通道处理器
     *
     * @return 通道处理器
     */
    ChannelHandler[] channelHandlers();
}
