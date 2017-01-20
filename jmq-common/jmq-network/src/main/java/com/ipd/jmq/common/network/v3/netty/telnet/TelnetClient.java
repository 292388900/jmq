package com.ipd.jmq.common.network.v3.netty.telnet;

import com.ipd.jmq.common.network.ClientConfig;
import com.ipd.jmq.common.network.CommandHandlerFactory;
import com.ipd.jmq.common.network.netty.AbstractClient;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * Created by zhangkepeng on 16-11-23.
 */
public class TelnetClient extends AbstractClient {

    public TelnetClient(ClientConfig config) {
        super(config, null, null, null);
    }

    public TelnetClient(ClientConfig config, EventLoopGroup ioLoopGroup, EventExecutorGroup workerGroup) {
        super(config, ioLoopGroup, workerGroup, null);
    }

    public TelnetClient(ClientConfig config, EventLoopGroup ioLoopGroup, EventExecutorGroup workerGroup,
                        CommandHandlerFactory factory) {
        super(config, ioLoopGroup, workerGroup, factory);
    }

    @Override
    protected ChannelHandler[] createChannelHandlers() {
        return new ChannelHandler[]{new TelnetClientDecoder(),new TelnetClientEncoder(), commandInvocation};
    }
}
