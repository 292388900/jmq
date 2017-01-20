package com.ipd.jmq.common.network;

import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.toolkit.lang.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * 请求命令请求处理器
 */
public class RequestHandler implements CommandHandler {
    protected static Logger logger = LoggerFactory.getLogger(RequestHandler.class);
    // 命令处理器工厂
    protected CommandHandlerFactory factory;
    // 异常处理器
    protected ExceptionHandler errorHandler;
    // 声明周期对象
    protected LifeCycle parent;

    public RequestHandler(CommandHandlerFactory factory, ExceptionHandler errorHandler, LifeCycle parent) {
        this.factory = factory;
        this.errorHandler = errorHandler;
        this.parent = parent;
    }

    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {
        if (factory == null) {
            return null;
        }
        // 获取处理器，心跳命令，不支持的命令也需要返回默认处理器
        CommandHandler handler = factory.getHandler(command);
        try {
            UserHandler task = new UserHandler(transport, command, handler, errorHandler, parent);
            // 获取线程池
            ExecutorService executorService = null;
            if (handler instanceof ThreadProvier) {
                executorService = ((ThreadProvier) handler).getExecutorService(command);
            }
            if (executorService == null) {
                // 如果没用线程池，则在当前线程池中执行
                task.run();
            } else {
                executorService.submit(task);
            }
        } catch (RejectedExecutionException e) {
            if (errorHandler != null) {
                errorHandler.process(transport, command, TransportException.ThreadExhaustException.build(), parent);
            }
        }
        return null;
    }

    /**
     * 执行处理器，发送应答任务
     */
    protected static class UserHandler implements Runnable {
        protected Transport transport;
        protected Command request;
        protected CommandHandler handler;
        protected ExceptionHandler errorHandler;
        protected LifeCycle parent;

        public UserHandler(final Transport transport, final Command request, final CommandHandler handler,
                           final ExceptionHandler errorHandler, final LifeCycle parent) {
            this.transport = transport;
            this.request = request;
            this.handler = handler;
            this.errorHandler = errorHandler;
            this.parent = parent;
        }

        @Override
        public void run() {
            // 处理请求命令
            Command response;
            try {
                response = handler.process(transport, request);
                // 判断应答是否为空
                if (response != null) {
                    transport.acknowledge(request, response, null);
                }
            } catch (Throwable e) {
                if (errorHandler != null) {
                    if (e instanceof TransportException) {
                        errorHandler.process(transport, request, (TransportException) e, parent);
                    } else if (e instanceof Exception) {
                        errorHandler.process(transport, request, (Exception) e, parent);
                    } else {
                        errorHandler.process(transport, request, TransportException.UnknownException.build(e), parent);
                    }
                }
            }
        }
    }
}
