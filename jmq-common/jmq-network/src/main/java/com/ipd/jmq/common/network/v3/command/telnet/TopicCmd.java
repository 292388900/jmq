package com.ipd.jmq.common.network.v3.command.telnet;

import com.ipd.jmq.common.telnet.Commands;

/**
 * Topic命令
 *
 * @author songzhimao
 * @date 2017/7/13
 */
public class TopicCmd extends TelnetCmd {

    public TopicCmd(StringBuilder builder) {
        super(builder);
    }

    @Override
    String type() {
        return Commands.TOPIC;
    }

    @Override
    public String cmd() {
        return cmd;
    }
}
