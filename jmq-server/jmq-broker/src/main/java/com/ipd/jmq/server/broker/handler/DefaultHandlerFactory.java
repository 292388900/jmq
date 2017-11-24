package com.ipd.jmq.server.broker.handler;

import com.ipd.jmq.common.network.CommandHandler;
import com.ipd.jmq.common.network.CommandHandlerFactory;
import com.ipd.jmq.common.network.v3.command.Command;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 默认命令处理器.
 *
 * @author lindeqiang
 * @since 2016/7/21 18:14
 */
public class DefaultHandlerFactory implements CommandHandlerFactory {
    // 命令处理器
    protected ConcurrentMap<Integer, CommandHandler> handlers = new ConcurrentHashMap<Integer, CommandHandler>();

    public DefaultHandlerFactory() {
    }

    /**
     * 注册命令
     *
     * @param handler
     * @return
     */
    public DefaultHandlerFactory register(final CommandHandler handler) {
        int[] types = ((Types) handler).type();
        for (Integer type : types) {
            handlers.putIfAbsent(type, handler);
        }
        return this;
    }


    @Override
    public CommandHandler getHandler(Command command) {
        CommandHandler commandHandler = null;
        if (command != null) {
            com.ipd.jmq.common.network.Types types = (com.ipd.jmq.common.network.Types) command.getPayload();
            commandHandler = handlers.get(types.type());
        }

        return commandHandler;
    }
}
