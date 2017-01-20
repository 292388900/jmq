package com.ipd.jmq.common.network.netty;

/**
 * Created by hexiaofeng on 16-6-23.
 */

import com.ipd.jmq.common.network.Config;
import com.ipd.jmq.common.network.RequestHandler;
import com.ipd.jmq.common.network.ResponseHandler;
import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.common.network.command.Direction;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * 命令处理器
 */
@ChannelHandler.Sharable
public class CommandInvocation<C extends Config> extends SimpleChannelInboundHandler<Command> {
    protected RequestHandler requestHandler;
    protected ResponseHandler<C> responseHandler;

    public CommandInvocation(RequestHandler requestHandler, ResponseHandler<C> responseHandler) {
        if (requestHandler == null) {
            throw new IllegalArgumentException("requestHandler can not be null.");
        } else if (responseHandler == null) {
            throw new IllegalArgumentException("responseHandler can not be null.");
        }
        this.requestHandler = requestHandler;
        this.responseHandler = responseHandler;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final Command command) throws Exception {
        // 如果传输的命令为空， 不做处理
        if (command == null) {
            return;
        }
        ChannelTransport transport = new InnerTransport(ctx.channel(), responseHandler.getBarrier());
        Direction direction = command.getHeader().getDirection();
        switch (direction) {
            case REQUEST: { // 如果是请求命令， 做请求处理
                requestHandler.process(transport, command);
                break;
            }
            case RESPONSE: { // 如果是响应命令， 做响应处理
                responseHandler.process(transport, command);
                break;
            }
        }
    }
}
