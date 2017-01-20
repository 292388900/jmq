package com.ipd.jmq.common.model;

/**
 * Created by lining on 16-11-18.
 */
public class ExtendTask {
    //需要扩容的Topic
    private String topic;
    //需要扩容到的目的分组,分号分割。
    private String groups;
    //扩容的groups数量,如果groups已设置,则该属性不起作用.
    private int extendSize;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroups() {
        return groups;
    }

    public void setGroups(String groups) {
        this.groups = groups;
    }

    public int getExtendSize() {
        return extendSize;
    }

    public void setExtendSize(int extendSize) {
        this.extendSize = extendSize;
    }
}
