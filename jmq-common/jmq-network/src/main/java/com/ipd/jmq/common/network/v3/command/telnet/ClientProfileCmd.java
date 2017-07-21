package com.ipd.jmq.common.network.v3.command.telnet;

import com.ipd.jmq.common.telnet.Commands;

/**
 * ClientProfileCmd
 *
 * @author luoruiheng
 * @since 1/5/17
 */
public class ClientProfileCmd extends TelnetCmd {

    public ClientProfileCmd(StringBuilder builder) {
        super(builder);
    }

    @Override
    String type() {
        return Commands.CLIENTPROFILE;
    }

    @Override
    public String cmd() {
        return cmd;
    }
}
