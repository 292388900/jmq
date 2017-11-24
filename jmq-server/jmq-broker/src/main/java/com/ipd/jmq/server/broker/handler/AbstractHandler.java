package com.ipd.jmq.server.broker.handler;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.server.broker.SessionManager;
import com.ipd.jmq.common.network.CommandHandler;
import com.ipd.jmq.common.network.ThreadProvier;
import java.util.concurrent.ExecutorService;

/**
 * 命令处理器基类.
 *
 * @author lindeqiang
 * @since 2016/7/26 8:11
 */
public abstract class AbstractHandler implements CommandHandler, ThreadProvier {
    protected ExecutorService executorService;
    protected SessionManager sessionManager;
    protected Broker broker;





    @Override
    public ExecutorService getExecutorService(final Command command) {
        return executorService;
    }
}
