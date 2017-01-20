package com.ipd.jmq.server.broker.netty;

import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.network.*;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.CommandUtils;
import com.ipd.jmq.common.network.v3.command.JMQHeader;
import com.ipd.jmq.common.network.v3.command.JMQPayload;
import com.ipd.jmq.server.broker.netty.protocol.MessagingProtocol;
import com.ipd.jmq.common.model.Acknowledge;
import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.common.network.command.Direction;
import com.ipd.jmq.common.network.netty.AbstractServer;
import com.ipd.jmq.common.network.netty.CommandInvocation;
import com.ipd.jmq.common.network.netty.Protocol;
import com.ipd.jmq.common.network.netty.ProtocolDecoder;
import com.ipd.jmq.toolkit.lang.LifeCycle;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.List;

/**
 * JMQ消息服务.
 *
 * @author lindeqiang
 * @since 2016/7/21 14:53
 */
public class MessagingServer extends AbstractServer {
    final private List<Protocol> protocols;

    public MessagingServer(ServerConfig config, EventLoopGroup bossLoopGroup, EventLoopGroup ioLoopGroup, EventExecutorGroup workerGroup, List<Protocol> protocols) {
        super(config, bossLoopGroup, ioLoopGroup, workerGroup, null);
        this.protocols = protocols;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();

        if (protocols != null) {
            for (Protocol protocol: protocols) {
                if (protocol instanceof MessagingProtocol) {
                    MessagingProtocol messagingProtocol = (MessagingProtocol) protocol;
                    messagingProtocol.setCommandInvocation(new CommandInvocation(
                            new RequestHandler(messagingProtocol.getFactory(), errorHandler, this),
                            new ResponseHandler(asyncExecutor, barrier, errorHandler)));
                    messagingProtocol.setConnectionHandler(connectionHandler);
                }
            }
        }
    }

    @Override
    protected ChannelHandler[] createChannelHandlers() {
        return new ChannelHandler[]{new ProtocolDecoder(workerGroup, protocols)};
    }

    @Override
    protected CommandHandler createHeartbeat() {
        return new DefaultHeartbeatHandler();
    }

    @Override
    protected ExceptionHandler createErrorHandler() {
        return new DefaultErrorHandler();
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

    /**
     * JMQ默认的异常处理器
     */
    protected static class DefaultErrorHandler implements ExceptionHandler {

        @Override
        public void process(Transport transport, Command command, Exception e, LifeCycle lifeCycle) {
            // 命令实体
            Object payload = command.getPayload();

            if (payload instanceof JMQPayload) {
                JMQHeader header = (JMQHeader) command.getHeader();
                if (lifeCycle.isStarted()) {
                    logger.error("process command error. type=" + header.getType() + ",payload=[" + command.getPayload()
                            + "],\n", e);
                }
                try {
                    if (header.getAcknowledge() != Acknowledge.ACK_NO) {
                        int code = JMQCode.CN_UNKNOWN_ERROR.getCode();
                        if (e instanceof JMQException) {
                            code = ((JMQException) e).getCode();
                        }
                        transport.oneway(CommandUtils.createBooleanAck(header.getRequestId(), code, e.getMessage()));
                    }
                } catch (Throwable th) {
                    logger.error("process error failure", e);
                }
            } else {
                if (lifeCycle.isStarted()) {
                    logger.error("process command error. payload=" + payload.toString() + ",payload=[" + command
                            .getPayload()
                            + "],\n", e);
                }
                transport.stop();
            }

        }
    }
}
