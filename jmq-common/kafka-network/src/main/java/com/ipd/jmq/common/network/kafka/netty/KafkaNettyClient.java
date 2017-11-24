package com.ipd.jmq.common.network.kafka.netty;

import com.ipd.jmq.common.network.kafka.codec.KafkaCodec;
import com.ipd.jmq.common.network.ClientConfig;
import com.ipd.jmq.common.network.CommandHandlerFactory;
import com.ipd.jmq.common.network.v3.netty.AbstractClient;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * Created by zhangkepeng on 16-9-6.
 */
public class KafkaNettyClient extends AbstractClient {

    public KafkaNettyClient(ClientConfig config) {
        super(config);
    }

    public KafkaNettyClient(ClientConfig config, EventLoopGroup ioLoopGroup, EventExecutorGroup workerGroup) {
        super(config, ioLoopGroup, workerGroup);
    }

    public KafkaNettyClient(ClientConfig config, EventLoopGroup ioLoopGroup, EventExecutorGroup workerGroup, CommandHandlerFactory factory) {
        super(config, ioLoopGroup, workerGroup, factory);
    }

    @Override
    protected ChannelHandler[] createChannelHandlers() {
        return new ChannelHandler[]{new KafkaNettyDecoder(new KafkaCodec(config.getFrameMaxSize())), new KafkaNettyEncoder(new KafkaCodec(config.getFrameMaxSize())), connectionHandler, commandInvocation};
    }
}
