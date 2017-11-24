package com.ipd.jmq.common.network.v3.command.telnet;

import com.ipd.jmq.common.telnet.Commands;

/**
 * Created by zhangkepeng on 16-12-1.
 */
public class AuthCmd extends TelnetCmd{



    public AuthCmd(StringBuilder builder) {
        super(builder);
    }

    @Override
    public String cmd() {
        return cmd;
    }

    @Override
    public String type() {
        return Commands.AUTH;
    }

}
