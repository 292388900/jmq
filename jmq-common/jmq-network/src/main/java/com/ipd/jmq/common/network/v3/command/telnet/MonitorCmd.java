package com.ipd.jmq.common.network.v3.command.telnet;

import com.ipd.jmq.common.network.command.telnet.Commands;

/**
 * @author zhangkepeng
 * @since 16-12-1.
 */
public class MonitorCmd extends TelnetCmd {

    // params可以是字符串构造器，包含特定参数
    public MonitorCmd(StringBuilder builder) {
        super(builder);
    }

    @Override
    public String cmd() {
        return cmd;
    }

    @Override
    public String type() {
        return Commands.MONITOR;
    }

}
