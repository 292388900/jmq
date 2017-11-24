package com.ipd.jmq.common.network;


import com.ipd.jmq.common.network.v3.command.Command;

/**
 * 命令处理器工厂类
 */
public interface CommandHandlerFactory {

    /**
     * 获取处理器，心跳命令，不支持的命令也需要返回默认处理器
     *
     * @param command 命令
     */
    CommandHandler getHandler(Command command);

}