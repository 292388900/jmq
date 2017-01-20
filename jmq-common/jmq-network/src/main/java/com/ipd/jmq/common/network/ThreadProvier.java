package com.ipd.jmq.common.network;


import com.ipd.jmq.common.network.command.Command;

import java.util.concurrent.ExecutorService;

/**
 * 提供独立的线程池
 */
public interface ThreadProvier {

    /**
     * 获取处理当前命令的线程执行器
     *
     * @param command 发送的请求命令
     * @return 处理当前命令的线程执行器
     */
    ExecutorService getExecutorService(Command command);

}