package com.ipd.jmq.common.network.v3.netty.telnet.base;

import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.netty.Keys;
import com.ipd.jmq.toolkit.network.Ipv4;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Created by zhangkepeng on 16-11-25.
 */
public class TelnetChannelHandler extends ChannelDuplexHandler {

    protected static Logger logger = LoggerFactory.getLogger(TelnetChannelHandler.class);

    /**
     * 允许执行Telnet命令的连接，前面进行过admin或参数密码认证操作
     */
    public final static Set<Transport> AUTHENTICATED_TRANSPORTS = new ConcurrentSet<Transport>();

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        Channel channel = ctx.channel();
        channel.attr(Keys.INPUT).remove();
        Transport transport = channel.attr(Keys.TRANSPORT).get();
        AUTHENTICATED_TRANSPORTS.remove(transport);
        channel.attr(Keys.TRANSPORT).remove();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        Channel channel = ctx.channel();
        String address = Ipv4.toAddress(channel.remoteAddress());

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
