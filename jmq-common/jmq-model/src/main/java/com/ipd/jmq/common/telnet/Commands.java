package com.ipd.jmq.common.telnet;

/**
 * Created by zhangkepeng on 16-11-29.
 */
public class Commands {

    // 命令参数分隔符
    public static final String SEPARATOR = " ";

    // 权限认证
    public static final String AUTH = "auth";
    // 监控命令
    public static final String MONITOR = "monitor";
    // 权限查询命令
    public static final String PERMIQUERY = "permiquery";
    // client profile
    public static final String CLIENTPROFILE = "clientProfile";
    // 向zk设置topic信息 get/set
    public static final String TOPIC = "topic";
    // 向zk设置broker信息 get/set
    public static final String BROKER = "broker";
    //system manager
    public static final String SYSTEM = "system";

}
