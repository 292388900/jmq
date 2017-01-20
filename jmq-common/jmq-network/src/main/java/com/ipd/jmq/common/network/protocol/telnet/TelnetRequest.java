package com.ipd.jmq.common.network.protocol.telnet;


import com.ipd.jmq.common.network.command.Payload;

/**
 * Telnet协议
 * Created by hexiaofeng on 16-7-7.
 */
public class TelnetRequest implements Payload {
    // 原始请求消息体
    protected byte[] message;
    // 输入命令
    protected String input;
    // 解析后命令
    protected String command;
    // 解析后参数
    protected String[] args;

    public TelnetRequest(byte[] message) {
        this(message, null);
    }

    public TelnetRequest(String input) {
        this(null, input);
    }

    public TelnetRequest(byte[] message, String input) {
        this.message = message;
        this.input = input;
        if (input != null) {
            // 过滤掉不可见字符
            input = input.trim();
            this.input = input;
            if (!input.isEmpty()) {
                String[] values = input.split("\\s+");
                this.command = values[0];
                if (values.length > 1) {
                    this.args = new String[values.length - 1];
                    System.arraycopy(values, 1, args, 0, args.length);
                }
            }
        }
    }

    public String getInput() {
        return input;
    }

    public String getCommand() {
        return command;
    }

    public String[] getArgs() {
        return args;
    }

    public byte[] getMessage() {
        return message;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer(100);
        if (command != null) {
            sb.append(command);
        }
        if (args != null) {
            for (String arg : args) {
                sb.append(' ').append(arg);
            }
        }
        return sb.toString();
    }
}
