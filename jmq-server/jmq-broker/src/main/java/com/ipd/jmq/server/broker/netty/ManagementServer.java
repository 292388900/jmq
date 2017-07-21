package com.ipd.jmq.server.broker.netty;

import com.ipd.jmq.common.network.*;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.JMQHeader;
import com.ipd.jmq.server.broker.netty.protocol.ManagementProtocol;
import com.ipd.jmq.common.model.Acknowledge;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.common.network.v3.command.Direction;
import com.ipd.jmq.common.network.v3.netty.AbstractServer;
import com.ipd.jmq.common.network.v3.netty.ProtocolDecoder;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;


/**
 * JMQ管理服务.
 *
 * @author lindeqiang
 * @since 2016/7/21 14:53
 */
public class ManagementServer extends AbstractServer {
    public ManagementServer(ServerConfig config) {
        super(config);
    }

    public ManagementServer(ServerConfig config, EventLoopGroup bossLoopGroup, EventLoopGroup ioLoopGroup, EventExecutorGroup workerGroup) {
        super(config, bossLoopGroup, ioLoopGroup, workerGroup);
    }

    public ManagementServer(ServerConfig config, EventLoopGroup bossLoopGroup, EventLoopGroup ioLoopGroup, EventExecutorGroup workerGroup, CommandHandlerFactory factory) {
        super(config, bossLoopGroup, ioLoopGroup, workerGroup, factory);
    }

    @Override
    protected ChannelHandler[] createChannelHandlers() {
        return new ChannelHandler[]{new ProtocolDecoder(new ManagementProtocol(factory, ">", 5, null))};
    }

    @Override
    protected CommandHandler createHeartbeat() {
        return new DefaultHeartbeatHandler();
    }

    /**
     * 默认心跳处理器
     */
    protected static class DefaultHeartbeatHandler implements CommandHandler {
        @Override
        public Command process(Transport transport, Command command) throws TransportException {
            JMQHeader.Builder builder = JMQHeader.Builder.create().type(CmdTypes.HEARTBEAT).acknowledge(Acknowledge.ACK_NO)
                    .direction(Direction.REQUEST);
            command = new Command(builder.build(), null);
            transport.oneway(command);
            return null;
        }
    }

}
