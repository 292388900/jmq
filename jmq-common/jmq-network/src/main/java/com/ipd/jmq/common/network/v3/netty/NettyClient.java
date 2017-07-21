package com.ipd.jmq.common.network.v3.netty;

import com.ipd.jmq.common.network.v3.codec.JMQCodec;
import com.ipd.jmq.common.network.ClientConfig;
import com.ipd.jmq.common.network.CommandHandlerFactory;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * JMQ netty 客户端.
 *
 * @author lindeqiang
 * @since 2016/7/27 18:05
 */
public class NettyClient extends AbstractClient {
    public NettyClient(ClientConfig config) {
        super(config);
    }

    public NettyClient(ClientConfig config, EventLoopGroup ioLoopGroup, EventExecutorGroup workerGroup) {
        super(config, ioLoopGroup, workerGroup);
    }

    public NettyClient(ClientConfig config, EventLoopGroup ioLoopGroup, EventExecutorGroup workerGroup, CommandHandlerFactory factory) {
        super(config, ioLoopGroup, workerGroup, factory);
    }

    @Override
    protected ChannelHandler[] createChannelHandlers() {
        return new ChannelHandler[]{new NettyDecoder(new JMQCodec()), new NettyEncoder(new JMQCodec()), connectionHandler, commandInvocation};
    }
}
