package com.ipd.jmq.common.network.protocol.telnet;

import com.ipd.jmq.common.network.CommandHandler;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.command.Command;

/**
 * 退格键处理器
 * Created by hexiaofeng on 16-7-11.
 */
public class BackspaceHandler implements CommandHandler {

    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {
        TelnetInput input = (TelnetInput) transport.attr(TelnetInput.INPUT);
        String text;
        if (input.isEmpty()) {
            // 光标右移1位
            text = TelnetEscape.moveRight(1);
        } else {
            char ch = input.deleteLast();
            if (input.isDoubleByteChar(ch)) {
                text = new String(new byte[]{8, 32, 32, 8, 8});
            } else {
                text = TelnetEscape.deleteRight();
            }
        }
        return new Command(TelnetHeader.Builder.response(), new TelnetResponse(text, false, false));
    }
}
