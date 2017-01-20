package com.ipd.jmq.server.broker.netty.protocol;

import com.ipd.jmq.common.network.CommandHandlerFactory;
import com.ipd.jmq.common.network.netty.Protocol;
import com.ipd.jmq.toolkit.plugin.ServicePlugin;
import io.netty.channel.ChannelHandler;

/**
 * 消息协议.
 *
 * @author lindeqiang
 * @since 2016/12/15 14:16
 */
public interface MessagingProtocol extends Protocol, ServicePlugin {
    /**
     * 命令处理器
     *
     * @param commandInvocation
     */
    void setCommandInvocation(ChannelHandler commandInvocation);

    /**
     * 连接处理器
     *
     * @param connectionHandler
     */
    void setConnectionHandler(ChannelHandler connectionHandler);

    /**
     * 返回命令处理工厂
     */
    CommandHandlerFactory getFactory();

    /**
     * 设置命令处理工厂
     */
    void setFactory(CommandHandlerFactory factory);
}
