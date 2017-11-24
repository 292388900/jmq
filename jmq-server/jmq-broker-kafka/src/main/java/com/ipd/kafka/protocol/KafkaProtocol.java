package com.ipd.kafka.protocol;

import com.ipd.jmq.common.network.kafka.codec.KafkaCodec;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.netty.KafkaNettyDecoder;
import com.ipd.jmq.common.network.kafka.netty.KafkaNettyEncoder;
import com.ipd.jmq.server.broker.netty.protocol.MessagingProtocol;
import com.ipd.jmq.common.network.CommandHandlerFactory;
import com.ipd.jmq.common.network.v3.codec.Codec;
import com.ipd.jmq.toolkit.URL;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;

/**
 * Created by zhangkepeng on 16-7-26.
 */
public class KafkaProtocol implements MessagingProtocol {

    protected ChannelHandler commandInvocation;
    protected ChannelHandler connectionHandler;
    protected CommandHandlerFactory factory;
    private Codec codec = new KafkaCodec();

    public KafkaProtocol() {
    }

    public KafkaProtocol(ChannelHandler connectionHandler, ChannelHandler commandInvocation) {
        this.connectionHandler = connectionHandler;
        this.commandInvocation = commandInvocation;
    }

    @Override
    public boolean support(ByteBuf in) {
        if (in.readableBytes() < 12) {
            return false;
        } else {
            // 消息大小
            int size = in.getInt(0);
            // 消息类型
            short type = in.getShort(4);
            short version;
            // 版本
            version = in.getShort(6);

            if (size > 0 && ((type >= 0 && type < 255) || (type >= KafkaCommandKeys.UPDATE_TOPICS_BROKER
                && type < (KafkaCommandKeys.BASE + 255))) && version < 3) {
                return true;
            }
        }

        return false;
    }

    @Override
    public void setCommandInvocation(ChannelHandler commandInvocation) {
        this.commandInvocation = commandInvocation;
    }

    @Override
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

    @Override
    public boolean keeplive() {
        return true;
    }

    @Override
    public ChannelHandler[] channelHandlers() {
        return new ChannelHandler[]{new KafkaNettyDecoder(codec), new KafkaNettyEncoder(codec), connectionHandler, commandInvocation};
    }

    @Override
    public String getType() {
        return "kafka";
    }

    @Override
    public void setUrl(URL url) {

    }
}
