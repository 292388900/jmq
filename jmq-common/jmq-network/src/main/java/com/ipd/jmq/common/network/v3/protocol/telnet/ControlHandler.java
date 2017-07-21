package com.ipd.jmq.common.network.v3.protocol.telnet;

import com.ipd.jmq.common.network.CommandHandler;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.v3.command.Command;

/**
 * 控制命令处理器
 * Created by hexiaofeng on 16-7-11.
 */
public class ControlHandler implements CommandHandler {
    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {
        return null;
    }
}
