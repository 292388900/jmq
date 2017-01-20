package com.ipd.jmq.common.network;

/**
 * Created by hexiaofeng on 16-6-23.
 */

import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.common.network.command.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * 响应命令处理器
 */
public class ResponseHandler<C extends Config> implements CommandHandler {
    protected static Logger logger = LoggerFactory.getLogger(ResponseHandler.class);
    // 异步回调处理执行器
    protected ExecutorService executorService;
    // 栅栏
    protected RequestBarrier<C> barrier;
    // 异常处理器
    protected ExceptionHandler errorHandler;

    public ResponseHandler(ExecutorService executorService, RequestBarrier<C> barrier, ExceptionHandler errorHandler) {
        this.executorService = executorService;
        this.barrier = barrier;
        this.errorHandler = errorHandler;
    }

    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {
        Header header = command.getHeader();
        // 超时被删除了
        final ResponseFuture responseFuture = barrier.get(header.getRequestId());
        if (responseFuture == null) {
            if (logger.isInfoEnabled()) {
                logger.info(String.format("request is timeout %s", header));
            }
            return null;
        }
        // 设置应答
        responseFuture.setResponse(command);
        // 异步调用
        if (responseFuture.getCallback() != null) {
            boolean success = false;
            ExecutorService executor = this.executorService;
            if (executor != null) {
                try {
                    executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                responseFuture.onSuccess();
                            } catch (Throwable e) {
                                logger.error("execute callback error.", e);
                            } finally {
                                responseFuture.release();
                            }
                        }
                    });
                    success = true;
                } catch (Throwable e) {
                    logger.error("execute callback error.", e);
                }
            }

            if (!success) {
                try {
                    responseFuture.onSuccess();
                } catch (Throwable e) {
                    logger.error("execute callback error.", e);
                } finally {
                    responseFuture.release();
                }
            }
        } else {
            // 释放资源，不回调
            if (!responseFuture.release()) {
                // 已经被释放了
                return null;
            }
        }
        barrier.remove(header.getRequestId());
        return null;
    }

    public RequestBarrier<C> getBarrier() {
        return barrier;
    }
}
