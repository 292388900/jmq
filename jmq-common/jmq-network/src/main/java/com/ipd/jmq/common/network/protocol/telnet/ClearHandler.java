package com.ipd.jmq.common.network.protocol.telnet;

import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.command.Command;

/**
 * 帮助命令处理器
 * Created by hexiaofeng on 16-7-8.
 */
public class ClearHandler implements TelnetHandler<Transport> {

    protected static final String CLEAR = "clear";

    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {
        return new Command(TelnetHeader.Builder.response(), new TelnetResponse(TelnetEscape.clear(), false, true));
    }

    @Override
    public String command() {
        return CLEAR;
    }

    @Override
    public String help() {
        return "usage:clear";
    }
}
