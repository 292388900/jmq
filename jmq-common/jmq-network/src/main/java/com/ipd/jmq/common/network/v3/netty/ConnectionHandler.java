package com.ipd.jmq.common.network.v3.netty;

import com.ipd.jmq.common.network.*;
import com.ipd.jmq.toolkit.concurrent.EventBus;
import com.ipd.jmq.toolkit.network.Ipv4;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 连接监听器
 *
 * @param <C>
 */
@ChannelHandler.Sharable
public class ConnectionHandler<C extends Config> extends ChannelDuplexHandler {
    protected static Logger logger = LoggerFactory.getLogger(ConnectionHandler.class);
    // 事件管理器
    protected EventBus<TransportEvent> eventBus;
    // 空闲处理器
    protected CommandHandler heartbeatHandler;
    protected RequestBarrier<C> barrier;

    public ConnectionHandler(EventBus<TransportEvent> eventBus, CommandHandler heartbeatHandler,
                             RequestBarrier<C> barrier) {
        this.eventBus = eventBus;
        this.heartbeatHandler = heartbeatHandler;
        this.barrier = barrier;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        // 注册链接事件到EventManager
        eventBus.add(new TransportEvent(TransportEvent.EventType.CONNECT, new InnerTransport(ctx.channel(), barrier)));
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        // 注册断开链接事件到EventManager
        eventBus.add(new TransportEvent(TransportEvent.EventType.CLOSE, new InnerTransport(ctx.channel(), barrier)));
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state().equals(IdleState.ALL_IDLE)) {
                ChannelTransport transport = new InnerTransport(ctx.channel(), barrier);
                if (heartbeatHandler != null) {
                    heartbeatHandler.process(transport, null);
                }
                eventBus.add(new TransportEvent(TransportEvent.EventType.IDLE, transport));
            }
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        Channel channel = ctx.channel();
        String address = Ipv4.toAddress(channel.remoteAddress());
        // 注册断开链接事件到EventManager
        eventBus.add(
                new TransportEvent(TransportEvent.EventType.EXCEPTION, new InnerTransport(ctx.channel(), barrier)));
        // 等待连接关闭
        try {
            channel.close().await();
        } catch (InterruptedException ignored) {
        }
        if (TransportException.isClosed(cause)) {
            logger.error(String.format("netty channel exception %s,%s", address, cause.getMessage()));
        } else {
            logger.error(String.format("netty channel exception %s", address), cause);
        }
    }
}