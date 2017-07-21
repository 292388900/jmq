package com.ipd.jmq.common.network.v3.protocol.telnet;

import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.v3.command.Command;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 帮助命令处理器
 * Created by hexiaofeng on 16-7-8.
 */
public class HelpHandler implements TelnetHandler<Transport> {

    public static final String HELP = "help";
    protected Map<String, String> helps = new ConcurrentSkipListMap<String, String>();

    public HelpHandler() {
        helps.put("history", "usage:history");
    }

    /**
     * 增加帮助信息
     *
     * @param handler 处理器
     */
    public void register(final TelnetHandler handler) {
        if (handler != null) {
            helps.put(handler.command(), handler.help());
        }
    }

    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {
        TelnetRequest payload = (TelnetRequest) command.getPayload();
        String help;
        String[] args = payload.getArgs();
        // 无参数
        if (args == null || args.length == 0) {
            // 拼所有的帮助命令
            StringBuilder builder = new StringBuilder(100);
            for (Map.Entry<String, String> entry : helps.entrySet()) {
                builder.append(entry.getKey()).append(TelnetEscape.CR);
            }
            help = builder.toString();
        } else {
            help = helps.get(args[0]);
        }
        if (help == null) {
            help = String.format("%s command is not found.");
        }
        return new Command(TelnetHeader.Builder.response(), new TelnetResponse(help));
    }

    @Override
    public String command() {
        return HELP;
    }

    @Override
    public String help() {
        return "usage:help <command>";
    }
}
