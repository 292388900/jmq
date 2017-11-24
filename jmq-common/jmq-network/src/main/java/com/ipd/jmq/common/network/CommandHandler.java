package com.ipd.jmq.common.network;


import com.ipd.jmq.common.network.v3.command.Command;

/**
 * 命令处理器抽象，具体的处理器实现类用于处理特定的命令类型。
 * 因此每种类型的命令应实现对应自己的处理器以实现具体的业务逻辑。
 */
public interface CommandHandler<T extends Transport> {

    /**
     * 命令处理方法，用于实现特定类型命令处理的逻辑。
     *
     * @param transport 通道
     * @param command   请求命令
     * @return 处理完请求后返回针对该请求的一个响应命令
     * @throws TransportException 处理过程中发生的所有异常在方法内部需要进行捕获并转化为TransportException抛出
     */
    Command process(T transport, Command command) throws TransportException;

}