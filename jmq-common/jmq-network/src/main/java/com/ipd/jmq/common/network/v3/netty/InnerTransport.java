package com.ipd.jmq.common.network.v3.netty;

import com.ipd.jmq.common.network.Config;
import com.ipd.jmq.common.network.RequestBarrier;
import com.ipd.jmq.common.network.ResponseFuture;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.model.Acknowledge;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.common.network.v3.command.CommandCallback;
import com.ipd.jmq.common.network.v3.command.Header;
import com.ipd.jmq.toolkit.network.Ipv4;
import com.ipd.jmq.toolkit.time.SystemClock;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CountDownLatch;

/**
 * 内部传输通道
 * Created by hexiaofeng on 16-6-23.
 */
public class InnerTransport<C extends Config> implements ChannelTransport {
    protected static Logger logger = LoggerFactory.getLogger(InnerTransport.class);
    protected Channel channel;
    protected RequestBarrier<C> barrier;

    public InnerTransport(final Channel channel, final RequestBarrier<C> barrier) {
        this.channel = channel;
        this.barrier = barrier;
    }

    @Override
    public Channel channel() {
        return channel;
    }

    @Override
    public Command sync(final Command command) throws TransportException {
        return sync(command, 0);
    }

    @Override
    public Command sync(final Command command, final int timeout) throws TransportException {
        if (command == null) {
            throw new IllegalArgumentException("The argument command must not be null");
        }
        int sendTimeout = timeout <= 0 ? barrier.getSendTimeout() : timeout;
        // 同步调用
        ResponseFuture future = new ResponseFuture(this, command, sendTimeout, null, null, new CountDownLatch(1));
        barrier.put(command.getHeader().getRequestId(), future);
        // 发送数据,应答成功回来或超时会自动释放command
        channel.writeAndFlush(command).addListener(new ResponseListener(future, barrier));

        try {
            // 等待命令返回
            Command response = future.await();
            if (null == response) {
                // 发送请求成功，等待应答超时
                if (future.isSuccess()) {
                    throw TransportException.RequestTimeoutException.build(Ipv4.toAddress(channel.remoteAddress()));
                } else {
                    // 发送请求失败
                    Throwable cause = future.getCause();
                    if (cause != null) {
                        throw cause;
                    }
                    throw TransportException.RequestErrorException.build();
                }
            }

            return response;
        } catch (Throwable e) {
            future.release(e, false);
            // 出现异常
            barrier.remove(command.getHeader().getRequestId());
            if (e instanceof TransportException) {
                throw (TransportException) e;
            } else if (e instanceof InterruptedException) {
                throw TransportException.InterruptedException.build();
            } else {
                throw TransportException.RequestErrorException.build(e);
            }
        }
    }

    @Override
    public void async(final Command command, final CommandCallback callback) throws TransportException {
        async(command, 0, callback);
    }

    @Override
    public void async(final Command command, final int timeout, final CommandCallback callback) throws
            TransportException {
        if (command == null) {
            throw new IllegalArgumentException("command must not be null");
        } else if (callback == null) {
            throw new IllegalArgumentException("callback must not be null");
        }

        int sendTimeout = timeout <= 0 ? barrier.getSendTimeout() : timeout;
        // 获取信号量
        try {
            long time = SystemClock.now();
            barrier.acquire(RequestBarrier.SemaphoreType.ASYNC, sendTimeout);
            time = SystemClock.now() - time;
            sendTimeout = (int) (sendTimeout - time);
            sendTimeout = sendTimeout < 0 ? 0 : sendTimeout;

            // 发送请求
            ResponseFuture future =
                    new ResponseFuture(this, command, sendTimeout, callback, barrier.asyncSemaphore, null);
            barrier.put(command.getHeader().getRequestId(), future);
            // 应答回来的时候或超时会自动释放command
            channel.writeAndFlush(command).addListener(new ResponseListener(future, barrier));
        } catch (TransportException e) {
            command.release();
            throw e;
        }
    }

    @Override
    public void oneway(final Command command) throws TransportException {
        oneway(command, 0);
    }

    @Override
    public void oneway(final Command command, final int timeout) throws TransportException {
        if (command == null) {
            throw new IllegalArgumentException("The argument command must not be null");
        }

        // 不需要应答
        command.getHeader().setAcknowledge(Acknowledge.ACK_NO);

        int sendTimeout = timeout <= 0 ? barrier.getSendTimeout() : timeout;
        ResponseFuture future = null;
        try {
            long time = SystemClock.now();
            // 获取信号量
            barrier.acquire(RequestBarrier.SemaphoreType.ONEWAY, sendTimeout);
            time = SystemClock.now() - time;
            sendTimeout = (int) (sendTimeout - time);
            sendTimeout = sendTimeout < 0 ? 0 : sendTimeout;

            // 发送请求
            future = new ResponseFuture(this, command, sendTimeout, null, barrier.onewaySemaphore,
                    new CountDownLatch(1));
            // 命令执行成功或超时则会自动释放command
            channel.writeAndFlush(command).addListener(new OnewayListener(future));
            // 确保处理完成
            future.await();
            // 后续会在Listener中自动释放Future
            if (!future.isSuccess()) {
                // 发送请求失败
                Throwable cause = future.getCause();
                if (cause != null) {
                    if (cause instanceof TransportException) {
                        throw (TransportException) cause;
                    }
                    throw TransportException.RequestErrorException.build(cause);
                }
                throw TransportException.RequestErrorException.build();
            }
        } catch (TransportException e) {
            // 防止在acquireSemaphore获取异常
            command.release();
            throw e;
        } catch (InterruptedException e) {
            TransportException.InterruptedException ex = TransportException.InterruptedException.build();
            future.release(ex, false);
            throw ex;
        }
    }

    @Override
    public void acknowledge(final Command request, final Command response, final CommandCallback callback) throws
            TransportException {
        // 判断应答是否为空
        if (response == null) {
            return;
        }

        if (request != null) {
            Header header = request.getHeader();
            if (header != null) {
                response.getHeader().setRequestId(header.getRequestId());
                // 判断请求是否要应答
                if (header.getAcknowledge() == Acknowledge.ACK_NO) {
                    // 不用应答，释放资源
                    response.release();
                    // 回调
                    if (callback != null) {
                        try {
                            callback.onSuccess(request, response);
                        } catch (Exception ignored) {
                        }
                    }
                    return;
                }
            }
        }

        // 发送应答命令,不需要应答
        ResponseFuture responseFuture = new ResponseFuture(this, response, 0, callback, null, null);
        channel.writeAndFlush(response).addListener(new OnewayListener(responseFuture))
                .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    @Override
    public SocketAddress remoteAddress() {
        return channel.remoteAddress();
    }

    @Override
    public SocketAddress localAddress() {
        return channel.localAddress();
    }

    @Override
    public Object attr(final String key) {
        if (key == null) {
            return null;
        }
        return channel.attr(Keys.get(key)).get();
    }

    @Override
    public void attr(final String key, final Object value) {
        if (key == null) {
            return;
        }
        channel.attr(Keys.get(key)).set(value);
    }

    @Override
    public void stop() {
        channel.close();
    }

    /**
     * 异步请求监听器
     */
    protected static abstract class FutureListener implements ChannelFutureListener {
        protected ResponseFuture response;

        public FutureListener(ResponseFuture response) {
            this.response = response;
        }

        /**
         * 输出日志
         *
         * @param channel 通道
         */
        protected void logError(final Channel channel) {
            // 打印日志
            String error = "send a request command to " + Ipv4.toAddress(channel.remoteAddress()) + " failed.";
            Throwable cause = response.getCause();
            if (cause != null) {
                if (cause instanceof ClosedChannelException) {
                    // 连接关闭了，则忽略该异常
                } else {
                    logger.error(error, cause);
                }
            } else {
                logger.error(error);
            }
        }

    }

    /**
     * 等待应答监听器
     */
    protected static class ResponseListener<C extends Config> extends FutureListener {

        private RequestBarrier<C> barrier;

        public ResponseListener(ResponseFuture response, RequestBarrier<C> barrier) {
            super(response);
            this.barrier = barrier;
        }

        @Override
        public void operationComplete(final ChannelFuture future) throws Exception {
            // 获取命令监听器
            response.setSuccess(future.isSuccess());
            if (response.isSuccess()) {
                // 请求成功，等待应答回调，目前请求命令占用的资源可以释放了
                Command request = response.getRequest();
                if (request != null) {
                    request.release();
                }
            } else {
                // 出错
                response.setCause(future.cause());
                response.setResponse(null);
                response.release(null, true);

                barrier.remove(response.getRequestId());
                // 关闭连接
                Channel channel = future.channel();
                channel.close();
                // 输出日志
                logError(channel);
            }

        }


    }

    /**
     * Oneway监听器
     */
    protected static class OnewayListener extends FutureListener {

        public OnewayListener(ResponseFuture response) {
            super(response);
        }

        @Override
        public void operationComplete(final ChannelFuture future) throws Exception {
            // 获取命令监听器
            response.setSuccess(future.isSuccess());
            response.setCause(future.cause());
            response.setResponse(null);
            response.release(null, true);
            // 没有存放到futures中
            if (!response.isSuccess()) {
                Channel channel = future.channel();
                channel.close();
                logError(channel);
            }
        }
    }

}