package com.ipd.jmq.common.network.v3.netty.telnet.param;

import com.beust.jcommander.Parameter;

/**
 * Created by lining on 17-3-9.
 */
public class SystemParam {

    // 启动Broker
    public static final String OPERATION_TYPE = "-o";
    // 停止Broker
    public static final String BROKER_NAME = "-n";

    public static final String TIME_OUT = "-t";

    @Parameter(names = {"-o", "--operation"}, description = "user")
    private String type;

    @Parameter(names = {"-n", "--name"}, description = "brokerName")
    private String name;

    @Parameter(names = {"-t", "--timeOut"}, description = "timeOut")
    private int time;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }
}
