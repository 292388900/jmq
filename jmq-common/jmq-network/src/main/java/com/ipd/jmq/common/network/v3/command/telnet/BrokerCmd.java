package com.ipd.jmq.common.network.v3.command.telnet;

import com.ipd.jmq.common.telnet.Commands;

/**
 * broker相关命令
 * @author songzhimao
 * @date 2017/7/13
 */
public class BrokerCmd extends TelnetCmd {
    public BrokerCmd(StringBuilder builder) {
        super(builder);
    }

    @Override
    String type() {
        return Commands.BROKER;
    }

    @Override
    public String cmd() {
        return cmd;
    }
}
