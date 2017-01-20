package com.ipd.jmq.common.network.v3.netty.telnet.param;

import com.beust.jcommander.Parameter;

/**
 * Created by zhangkepeng on 16-12-12.
 * 参数-u/-p仅作为用户/密码参数，勿做其它使用
 */
public class PermiQueryParam {
    public static final String LEVEL = "-l";
    public static final String TOPIC = "-t";
    public static final String APP = "-a";

    @Parameter(names = { "-t", "--topic" })
    private String topic;

    @Parameter(names = { "-a", "--app" })
    private String app;

    @Parameter(names = { "-l", "--level" })
    private String level;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public static enum Level {
        BROKER("broker"),
        TOPIC("topic"),
        APP("app");

        private String level;

        public String getLevel() {
            return level;
        }

        Level(String level) {
            this.level = level;
        }
    }
}
