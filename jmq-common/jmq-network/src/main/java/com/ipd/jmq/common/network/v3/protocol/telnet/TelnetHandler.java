package com.ipd.jmq.common.network.v3.protocol.telnet;

import com.ipd.jmq.common.network.CommandHandler;
import com.ipd.jmq.common.network.Transport;

/**
 * Telnet命令处理器
 * Created by hexiaofeng on 16-7-7.
 */
public interface TelnetHandler<T extends Transport> extends CommandHandler<T> {

    /**
     * 命令
     *
     * @return
     */
    String command();

    /**
     * 帮助信息
     *
     * @return 帮助信息
     */
    String help();
}
