package com.ipd.jmq.common.network.v3.netty;

import com.ipd.jmq.common.network.CommandHandlerFactory;
import com.ipd.jmq.common.network.ServerConfig;
import com.ipd.jmq.toolkit.concurrent.NamedThreadFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * 服务
 */
public abstract class AbstractServer extends AbstractTransport<ServerConfig> {

    // 接受连接请求的线程池
    protected EventLoopGroup bossLoopGroup;
    // 服务端启动器
    protected ServerBootstrap bootStrap;
    // 是否是内部创建
    protected boolean createBossLoopGroup;


    /**
     * 构造函数
     *
     * @param config 配置
     */
    public AbstractServer(ServerConfig config) {
        this(config, null, null, null, null);
    }

    /**
     * 构造函数
     *
     * @param config        配置
     * @param bossLoopGroup 接受连接请求的线程池
     * @param ioLoopGroup   IO处理的线程池
     * @param workerGroup   命令处理线程池
     */
    public AbstractServer(ServerConfig config, EventLoopGroup bossLoopGroup, EventLoopGroup ioLoopGroup,
                          EventExecutorGroup workerGroup) {
        this(config, bossLoopGroup, ioLoopGroup, workerGroup, null);
    }

    /**
     * 构造函数
     *
     * @param config        配置
     * @param bossLoopGroup 接受连接请求的线程池
     * @param ioLoopGroup   IO处理的线程池
     * @param workerGroup   命令处理线程池
     * @param factory       命令工厂类
     */
    public AbstractServer(ServerConfig config, EventLoopGroup bossLoopGroup, EventLoopGroup ioLoopGroup,
                          EventExecutorGroup workerGroup, CommandHandlerFactory factory) {
        super(config, ioLoopGroup, workerGroup, factory);
        this.bossLoopGroup = bossLoopGroup;
    }

    public AbstractServer config(ServerConfig config) {
        this.config = config;
        return this;
    }

    public EventLoopGroup getBossLoopGroup() {
        return bossLoopGroup;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        bootStrap = new ServerBootstrap();
        configure(config);
        bootStrap.bind().sync();
        if (config.getIp() == null || config.getIp().isEmpty()) {
            logger.info(
                    String.format("%s is started. listen on %d", this.getClass().getSimpleName(), config.getPort()));
        } else {
            logger.info(String.format("%s is started. listen on %s:%d", this.getClass().getSimpleName(), config.getIp(),
                    config.getPort()));
        }
    }

    /**
     * 配置启动器
     *
     * @param config 服务端配置
     */
    protected void configure(ServerConfig config) {
        if (bossLoopGroup == null) {
            bossLoopGroup = createEventLoopGroup(config.getSelectorThreads(), new NamedThreadFactory("BossLoopGroup"));
            createBossLoopGroup = true;
        }
        // NIO事件通知机制不支持so_timeout
        bootStrap.group(bossLoopGroup, ioLoopGroup)
                .channel(this.config.isEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, config.isTcpNoDelay())
                .option(ChannelOption.SO_REUSEADDR, config.isReuseAddress())
                .option(ChannelOption.SO_KEEPALIVE, config.isKeepAlive())
                .option(ChannelOption.SO_LINGER, config.getSoLinger())
                .option(ChannelOption.SO_RCVBUF, config.getSocketBufferSize())
                .option(ChannelOption.SO_SNDBUF, config.getSocketBufferSize())
                .option(ChannelOption.SO_BACKLOG, config.getBacklog());
        if (config.getIp() == null || config.getIp().isEmpty()) {
            bootStrap.localAddress(config.getPort());
        } else {
            bootStrap.localAddress(config.getIp(), config.getPort());
        }
        bootStrap.childHandler(createChildHandler());
    }

    @Override
    protected void doStop() {
        if (createBossLoopGroup) {
            bossLoopGroup.shutdownGracefully();
            bossLoopGroup = null;
            createBossLoopGroup = false;
        }
        super.doStop();
    }

}