package com.ipd.jmq.common.network.v3.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.ArrayList;
import java.util.List;

/**
 * 协议决策
 * Created by hexiaofeng on 16-6-23.
 */
public class ProtocolDecoder extends ByteToMessageDecoder {

    protected List<Protocol> protocols;
    // 命令处理线程池
    protected EventExecutorGroup workerGroup;

    public ProtocolDecoder(List<Protocol> protocols) {
        this(null, protocols);
    }

    public ProtocolDecoder(Protocol... protocols) {
        this(null, protocols);
    }

    public ProtocolDecoder(EventExecutorGroup workerGroup, List<Protocol> protocols) {
        this.protocols = protocols;
        this.workerGroup = workerGroup;
    }

    public ProtocolDecoder(EventExecutorGroup workerGroup, Protocol... protocols) {
        this.workerGroup = workerGroup;
        this.protocols = new ArrayList<Protocol>();
        if (protocols != null) {
            for (Protocol protocol : protocols) {
                this.protocols.add(protocol);
            }
        }
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) throws Exception {
        if (protocols == null || protocols.isEmpty()) {
            return;
        }

        if (in.readableBytes() <= 0) {
            return;
        }
        // 遍历协议
        for (Protocol protocol : protocols) {
            if (protocol.support(in)) {
                // channel上绑定协议，便于后续逻辑处理
                ctx.channel().attr(Keys.PROTOCOL).set(protocol);

                // 添加后续的处理器
                ChannelPipeline pipeline = ctx.pipeline();
                if (workerGroup != null) {
                    pipeline.addLast(workerGroup, protocol.channelHandlers());
                } else {
                    pipeline.addLast(protocol.channelHandlers());
                }
                // 移除当前处理器，不用在判断了
                pipeline.remove(this);
                if (protocol.keeplive()) {
                    // 重新触发连接建立事件，第一个事件可能触发在decode方法后
                    pipeline.fireChannelRegistered();
                    pipeline.fireChannelActive();
                }
                return;
            }
        }

        //throw TransportException.InvalidProtocolException.build(); TODO 是否在超过一定长度后还未匹配上时抛出异常
    }
}
