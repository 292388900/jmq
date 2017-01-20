package com.ipd.jmq.common.network.v3.command.telnet;

/**
 * @author zhangkepeng
 * @since 16-12-1.
 */
public abstract class TelnetCmd {

    protected String cmd;

    public TelnetCmd(StringBuilder builder) {
        if (builder != null) {
            cmd = builder.toString();
        }
    }

    /**
     * 命令类型
     *
     * @return
     */
    abstract String type();

    /**
     * 请求命令
     */
    abstract public String cmd();
}
