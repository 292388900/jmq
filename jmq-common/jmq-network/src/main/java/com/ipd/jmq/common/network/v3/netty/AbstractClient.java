package com.ipd.jmq.common.network.v3.netty;

import com.ipd.jmq.common.network.*;
import com.ipd.jmq.toolkit.network.Ipv4;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.EventExecutorGroup;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

/**
 * Netty客户端
 */
public abstract class AbstractClient extends AbstractTransport<ClientConfig> implements Client {
    // Netty客户端启动器
    protected Bootstrap bootstrap;

    /**
     * 构造函数
     *
     * @param config 配置
     */
    public AbstractClient(ClientConfig config) {
        super(config, null, null, null);
    }

    /**
     * 构造函数
     *
     * @param config      配置
     * @param ioLoopGroup IO处理的线程池
     * @param workerGroup 命令处理线程池
     */
    public AbstractClient(ClientConfig config, EventLoopGroup ioLoopGroup, EventExecutorGroup workerGroup) {
        super(config, ioLoopGroup, workerGroup, null);
    }

    /**
     * 构造函数
     *
     * @param config      配置
     * @param ioLoopGroup IO处理的线程池
     * @param workerGroup 命令处理线程池
     * @param factory     命令工厂类
     */
    public AbstractClient(ClientConfig config, EventLoopGroup ioLoopGroup, EventExecutorGroup workerGroup,
                          CommandHandlerFactory factory) {
        super(config, ioLoopGroup, workerGroup, factory);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        // Netty客户端启动器
        bootstrap = new Bootstrap();
        bootstrap.group(ioLoopGroup).channel(config.isEpoll() ? EpollSocketChannel.class : NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectionTimeout())
                .option(ChannelOption.TCP_NODELAY, config.isTcpNoDelay())
                //.option(ChannelOption.SO_TIMEOUT, config.getSoTimeout()) // NIO事件通知机制不支持so_timeout
                .option(ChannelOption.SO_REUSEADDR, config.isReuseAddress())
                .option(ChannelOption.SO_KEEPALIVE, config.isKeepAlive())
                .option(ChannelOption.SO_LINGER, config.getSoLinger())
                .option(ChannelOption.SO_RCVBUF, config.getSocketBufferSize())
                .option(ChannelOption.SO_SNDBUF, config.getSocketBufferSize()).handler(createChildHandler());
    }

    @Override
    public Transport createTransport(final String address) throws TransportException {
        return createTransport(address, 0);
    }

    @Override
    public Transport createTransport(final String address, final long connectionTimeout) throws TransportException {
        if (address == null || address.isEmpty()) {
            throw new IllegalArgumentException("address must not be empty!");
        }
        String[] parts = address.split("[._:]");
        if (parts.length < 1) {
            throw new IllegalArgumentException("address is invalid.");
        }
        int port;
        try {
            port = Integer.parseInt(parts[parts.length - 1]);
            if (port < 0) {
                throw new IllegalArgumentException("address is invalid.");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("address is invalid.");
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < parts.length - 1; i++) {
            if (i > 0) {
                builder.append('.');
            }
            builder.append(parts[i]);
        }
        String ip = builder.toString();
        try {
            return createTransport(new InetSocketAddress(InetAddress.getByName(ip), port), connectionTimeout);
        } catch (TransportException e) {
            throw e;
        } catch (UnknownHostException e) {
            throw TransportException.UnknownHostException.build(ip);
        }
    }

    @Override
    public Transport createTransport(final SocketAddress address) throws TransportException {
        return createTransport(address, 0);
    }

    @Override
    public Transport createTransport(final SocketAddress address, final long connectionTimeout) throws
            TransportException {
        if (address == null) {
            throw new IllegalArgumentException("address must not be null!");
        }
        long timeout = connectionTimeout > 0 ? connectionTimeout : config.getConnectionTimeout();
        String addr = Ipv4.toAddress(address);
        ChannelFuture channelFuture;
        Channel channel = null;
        try {
            channelFuture = bootstrap.connect(address);
            if (!channelFuture.await(timeout)) {
                throw TransportException.ConnectionTimeoutException.build(addr);
            }
            channel = channelFuture.channel();
            if (channel == null || !channel.isActive()) {
                throw TransportException.ConnectionException.build(addr);
            }
            return createTransport(channel);
        } catch (InterruptedException e) {
            throw TransportException.InterruptedException.build();
        }
    }
}