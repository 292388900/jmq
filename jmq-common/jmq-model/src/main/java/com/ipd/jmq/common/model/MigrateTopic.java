package com.ipd.jmq.common.model;

/**
 * Created by lining on 16-11-21.
 */
public class MigrateTopic {

    //要迁移的Topic
    private String topic;
    //迁移的源Broker
    private String fGroup;
    //迁移的目的Broker
    private String tGroup;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getfGroup() {
        return fGroup;
    }

    public void setfGroup(String fGroup) {
        this.fGroup = fGroup;
    }

    public String gettGroup() {
        return tGroup;
    }

    public void settGroup(String tGroup) {
        this.tGroup = tGroup;
    }
}
