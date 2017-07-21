package com.ipd.jmq.common.network.v3.protocol.telnet;

import com.ipd.jmq.common.network.CommandHandler;
import com.ipd.jmq.common.network.CommandHandlerFactory;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.v3.command.Command;

/**
 * 翻阅处理器
 * Created by hexiaofeng on 16-7-11.
 */
public class EnterHandler implements CommandHandler {

    // 命令执行器
    protected CommandHandlerFactory factory;

    public EnterHandler(CommandHandlerFactory factory) {
        this.factory = factory;
    }

    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {
        TelnetInput input = (TelnetInput) transport.attr(TelnetInput.INPUT);
        TelnetRequest payload = (TelnetRequest) command.getPayload();
        String text = payload.getInput();
        Command request = command;
        if (!input.isEmpty()) {
            text = input.append(text).getInput();
            input.delete();
            request = new Command(command.getHeader(), new TelnetRequest(payload.getMessage(), text));
        }
        if (text.isEmpty()) {
            return new Command(TelnetHeader.Builder.response(), new TelnetResponse(""));
        } else {
            // 按序号执行历史命令
            if (text.charAt(0) == TelnetInput.CMD_INDEX) {
                String history = input.getHistory(text);
                if (history == null) {
                    // 历史命令没有找到
                    return new Command(TelnetHeader.Builder.response(),
                            new TelnetResponse(String.format("%s:text is not found.", text)));
                }
                text = history;
                request = new Command(TelnetHeader.Builder.response(), new TelnetRequest(payload.getMessage(), text));
            }
            if (TelnetInput.CMD_HISTORY.equals(text)) {
                //历史命令
                StringBuilder builder = new StringBuilder(1000);
                if (!input.getHistories().isEmpty()) {
                    for (TelnetInput.History history : input.getHistories()) {
                        builder.append(history.id).append('\t').append(history.command).append(TelnetEscape.CR);
                    }
                }
                input.addHistory(text);
                return new Command(TelnetHeader.Builder.response(), new TelnetResponse(builder.toString()));
            } else {
                input.addHistory(text);
                // 根据命令去查找
                CommandHandler handler = factory.getHandler(request);
                if (handler == null) {
                    // 历史命令没有找到
                    return new Command(TelnetHeader.Builder.response(),
                            new TelnetResponse(String.format("%s:text is not found.", text)));
                } else {
                    return handler.process(transport, request);
                }
            }
        }
    }

}
