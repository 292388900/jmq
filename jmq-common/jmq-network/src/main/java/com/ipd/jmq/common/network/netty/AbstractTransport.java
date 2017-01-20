package com.ipd.jmq.common.network.netty;

import com.ipd.jmq.common.network.*;
import com.ipd.jmq.toolkit.concurrent.EventBus;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.concurrent.NamedThreadFactory;
import com.ipd.jmq.toolkit.service.Service;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Netty传输基础类
 */
public abstract class AbstractTransport<C extends Config> extends Service {
    protected static Logger logger = LoggerFactory.getLogger(AbstractTransport.class);

    // 异步回调处理执行器
    protected ExecutorService asyncExecutor;
    // 传输配置
    protected C config;
    // 事件管理器
    protected EventBus<TransportEvent> transportEvent = new EventBus<TransportEvent>("TransportEvent");
    // 命令处理器工厂
    protected CommandHandlerFactory factory;
    // 栅栏
    protected RequestBarrier<C> barrier;
    // IO处理线程池
    protected EventLoopGroup ioLoopGroup;
    // 命令处理线程池
    protected EventExecutorGroup workerGroup;
    // 定时器
    protected Timer timer;
    // 心跳处理器
    protected CommandHandler heartbeatHandler;
    // 异常处理器
    protected ExceptionHandler errorHandler;
    // 连接处理器
    protected ChannelHandler connectionHandler;
    // 命令调用
    protected ChannelHandler commandInvocation;
    // 是否是内部创建的IO处理线程池
    protected boolean createIoLoopGroup;

    /**
     * 构造函数
     *
     * @param config 配置
     */
    public AbstractTransport(C config) {
        this(config, null, null, null);
    }

    /**
     * 构造函数，传入workerLoopGroup和eventExecutorGroup是为了优化
     *
     * @param config      配置
     * @param ioLoopGroup IO处理线程池
     * @param workerGroup 命令处理线程
     * @param factory     命令工厂
     */
    public AbstractTransport(C config, EventLoopGroup ioLoopGroup, EventExecutorGroup workerGroup,
                             CommandHandlerFactory factory) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        this.config = config;
        this.ioLoopGroup = ioLoopGroup;
        this.workerGroup = workerGroup;
        this.factory = factory;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        heartbeatHandler = createHeartbeat();
        errorHandler = createErrorHandler();
        barrier = new RequestBarrier<C>(config);
        connectionHandler = new ConnectionHandler(transportEvent, heartbeatHandler, barrier);
        commandInvocation = new CommandInvocation(new RequestHandler(factory, errorHandler, this),
                new ResponseHandler(asyncExecutor, barrier, errorHandler));
        if (ioLoopGroup == null) {
            ioLoopGroup = createEventLoopGroup(config.getWorkerThreads(), new NamedThreadFactory("IoLoopGroup"));
            createIoLoopGroup = true;
        }
        asyncExecutor =
                Executors.newFixedThreadPool(config.getCallbackThreads(), new NamedThreadFactory("AsyncCallback"));
        timer = new Timer("ClearTimeout");
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (!isStarted()) {
                    return;
                }
                barrier.evict();
            }
        }, 1000 * 3, 1000);
        transportEvent.start();
    }

    @Override
    protected void doStop() {
        super.doStop();
        if (timer != null) {
            timer.cancel();
        }

        if (createIoLoopGroup) {
            ioLoopGroup.shutdownGracefully();
            ioLoopGroup = null;
            createIoLoopGroup = false;
        }

        asyncExecutor.shutdown();
        asyncExecutor = null;

        // 关闭所有Future
        barrier.clear();
        transportEvent.stop(true);
    }

    /**
     * 创建事件循环组，尽量共享
     *
     * @param threads       线程数
     * @param threadFactory 线程工厂类
     * @return 事件组
     */
    protected EventLoopGroup createEventLoopGroup(int threads, ThreadFactory threadFactory) {
        if (config.isEpoll()) {
            return new EpollEventLoopGroup(threads, threadFactory);
        }
        return new NioEventLoopGroup(threads, threadFactory);
    }

    /**
     * 创建通道
     *
     * @param channel 通道
     * @return 通道
     */
    public ChannelTransport createTransport(final Channel channel) {
        return new InnerTransport(channel, barrier);
    }

    /**
     * 增加监听器
     *
     * @param listener 监听器
     */
    public void addListener(final EventListener<TransportEvent> listener) {
        transportEvent.addListener(listener);
    }

    /**
     * 删除监听器
     *
     * @param listener 监听器
     */
    public void removeListener(final EventListener<TransportEvent> listener) {
        transportEvent.removeListener(listener);
    }

    public CommandHandlerFactory getFactory() {
        return this.factory;
    }

    public void setFactory(CommandHandlerFactory factory) {
        this.factory = factory;
    }

    public C getConfig() {
        return config;
    }

    /**
     * 创建心跳器
     *
     * @return 心跳器
     */
    protected CommandHandler createHeartbeat() {
        return null;
    }

    /**
     * 创建异常处理器
     *
     * @return 异常处理器
     */
    protected ExceptionHandler createErrorHandler() {
        return null;
    }

    /**
     * 创建空闲处理器
     *
     * @return 空闲处理器
     */
    protected IdleStateHandler createIdleHandler() {
        return new IdleStateHandler(0, 0, config.getMaxIdleTime(), TimeUnit.MILLISECONDS);
    }

    /**
     * 获取初始化的通道处理器
     *
     * @return 初始化的通道处理器
     */
    protected abstract ChannelHandler[] createChannelHandlers();

    /**
     * 构造处理链
     *
     * @return 处理链
     */
    protected ChannelHandler createChildHandler() {
        return new PipelineChain();
    }

    @ChannelHandler.Sharable
    protected class PipelineChain extends ChannelInitializer {
        @Override
        protected void initChannel(final Channel channel) throws Exception {
            // 把Transport对象注册到属性里面，便于后面调用
            channel.attr(Keys.TRANSPORT).set(createTransport(channel));
            if (workerGroup != null) {
                channel.pipeline().addLast(workerGroup, createChannelHandlers());
            } else {
                channel.pipeline().addLast(createChannelHandlers());
            }
        }
    }

}
