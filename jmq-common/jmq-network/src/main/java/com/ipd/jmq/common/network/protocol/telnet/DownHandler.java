package com.ipd.jmq.common.network.protocol.telnet;

import com.ipd.jmq.common.network.CommandHandler;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.command.Command;

/**
 * 翻阅处理器
 * Created by hexiaofeng on 16-7-11.
 */
public class DownHandler implements CommandHandler {
    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {
        TelnetInput input = (TelnetInput) transport.attr(TelnetInput.INPUT);
        String text = input.roll(false);
        return new Command(TelnetHeader.Builder.response(), new TelnetResponse(text, false, false));
    }

}
