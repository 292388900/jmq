package com.ipd.jmq.common.network.v3.command.telnet;

import com.ipd.jmq.common.network.command.telnet.Commands;

/**
 * Created by zhangkepeng on 16-12-12.
 */
public class PermiQueryCmd extends TelnetCmd{

    public PermiQueryCmd(StringBuilder builder) {
        super(builder);
    }

    @Override
    public String cmd() {
        return cmd;
    }

    @Override
    public String type() {
        return Commands.PERMIQUERY;
    }
}
