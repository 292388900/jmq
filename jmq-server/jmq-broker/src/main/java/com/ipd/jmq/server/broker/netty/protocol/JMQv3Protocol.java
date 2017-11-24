package com.ipd.jmq.server.broker.netty.protocol;

import com.ipd.jmq.common.network.v3.codec.JMQCodec;
import com.ipd.jmq.common.network.v3.netty.NettyDecoder;
import com.ipd.jmq.common.network.v3.netty.NettyEncoder;
import com.ipd.jmq.common.network.CommandHandlerFactory;
import com.ipd.jmq.toolkit.URL;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;

/**
 * Octopus协议.
 *
 * @author lindeqiang
 * @since 2016/7/21 16:17
 */
public class JMQv3Protocol implements MessagingProtocol {
    // 命令处理器
    protected ChannelHandler commandInvocation;
    // 连接处理器
    protected ChannelHandler connectionHandler;
    protected CommandHandlerFactory factory;

    public JMQv3Protocol() {
    }

    public void setCommandInvocation(ChannelHandler commandInvocation) {
        this.commandInvocation = commandInvocation;
    }

    public void setConnectionHandler(ChannelHandler connectionHandler) {
        this.connectionHandler = connectionHandler;
    }

    @Override
    public CommandHandlerFactory getFactory() {
        return factory;
    }

    @Override
    public void setFactory(CommandHandlerFactory factory) {
        this.factory = factory;
    }

    public JMQv3Protocol(final ChannelHandler connectionHandler, final ChannelHandler commandInvocation) {
        this.connectionHandler = connectionHandler;
        this.commandInvocation = commandInvocation;
    }

    @Override
    public boolean support(ByteBuf buf) {
        if (buf.readableBytes() < 2) {
            return false;
        } else {
            short magic = buf.getShort(0);
            if (magic == JMQCodec.MAGIC) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean keeplive() {
        return true;
    }

    @Override
    public ChannelHandler[] channelHandlers() {
        return new ChannelHandler[]{
                new NettyDecoder(new JMQCodec()), //JMQ解码器
                new NettyEncoder(new JMQCodec()), //JMQ编码器
                connectionHandler,   //连接处理器
                commandInvocation};  //命令处理器
    }

    @Override
    public String getType() {
        return "JMQv3";
    }

    @Override
    public void setUrl(URL url) {
        // do nothing
    }
}
