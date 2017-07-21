package com.ipd.jmq.common.network.v3.protocol.telnet;


import com.ipd.jmq.common.network.v3.command.Payload;

/**
 * Telnet应答
 * Created by hexiaofeng on 16-7-7.
 */
public class TelnetResponse implements Payload {
    // 消息
    protected String message;
    // 追加回车
    protected boolean newLine;
    // 追加提示符
    protected boolean prompt;

    public TelnetResponse(String message) {
        this(message, true, true);
    }

    public TelnetResponse(String message, boolean newLine, boolean prompt) {
        this.message = message;
        this.newLine = newLine;
        this.prompt = prompt;
    }

    public String getMessage() {
        return message;
    }

    public boolean isNewLine() {
        return newLine;
    }

    public boolean isPrompt() {
        return prompt;
    }
}
