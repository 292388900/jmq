package com.ipd.jmq.server.broker.handler;

import com.ipd.jmq.common.network.CommandHandler;
import com.ipd.jmq.common.network.CommandHandlerFactory;
import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.common.network.protocol.telnet.TelnetHandler;
import com.ipd.jmq.common.network.protocol.telnet.TelnetRequest;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * telnet 命令处理器工厂
 *
 * @author lindeqiang
 * @since 2016/7/21 18:14
 */
public class TelnetHandlerFactory implements CommandHandlerFactory {
    // 命令处理器
    protected ConcurrentMap<Object, CommandHandler> handlers = new ConcurrentHashMap<Object, CommandHandler>();

    /**
     * 命令注册
     *
     * @param handler
     * @return
     */
    public TelnetHandlerFactory register(final CommandHandler handler) {
        if (handler != null) {
            if (handler instanceof TelnetHandler) {
                String command = ((TelnetHandler) handler).command();
                handlers.putIfAbsent(command, handler);
            } else {
                throw new IllegalStateException("Do not support this kind of command handler：" + handler);
            }
        }
        return this;
    }

    @Override
    public CommandHandler getHandler(Command command) {
        // 命令实体
        Object payload = command.getPayload();
        if (payload instanceof TelnetRequest) {
            String cmd = ((TelnetRequest) payload).getCommand();
            return handlers.get(cmd);
        }

        return null;
    }
}
