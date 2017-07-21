package com.ipd.jmq.common.network.v3.protocol.telnet;

import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.v3.command.Command;

/**
 * 帮助命令处理器
 * Created by hexiaofeng on 16-7-8.
 */
public class ExitHandler implements TelnetHandler<Transport> {

    protected static final String EXIT = "exit";

    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {
        transport.stop();
        return null;
    }

    @Override
    public String command() {
        return EXIT;
    }

    @Override
    public String help() {
        return "usage:exit";
    }
}
