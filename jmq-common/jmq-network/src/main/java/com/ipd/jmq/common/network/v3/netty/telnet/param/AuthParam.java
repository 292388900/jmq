package com.ipd.jmq.common.network.v3.netty.telnet.param;

import com.beust.jcommander.Parameter;

/**
 * Created by zhangkepeng on 16-11-29.
 */
public class AuthParam {
    public static final String USER = "-u";
    public static final String PASSWORD = "-p";

    @Parameter(names = {"-u", "--user"}, description = "user")
    private String user;

    @Parameter(names = {"-p", "--password"}, description = "password")
    private String password;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
