package com.ipd.jmq.common.network;

import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.toolkit.lang.LifeCycle;

/**
 * 异常处理器。
 */
public interface ExceptionHandler<T extends Transport> {

    /**
     * 异常处理器，用于发送异常信息
     *
     * @param transport 通道
     * @param command   请求命令
     * @param e         异常
     * @param lifeCycle 生命周期对象
     */
    void process(T transport, Command command, Exception e, LifeCycle lifeCycle);

}