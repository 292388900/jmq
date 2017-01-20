package com.ipd.jmq.common.model;

/**
 * Created by zhangkepeng on 16-5-10.
 */
public class ControllerInfo extends BaseModel {

    // 主题
    private String topic;
    // 应用
    private String app;
    // 分组
    private String group;
    // 控制类型
    private String controllerType;
    // 控制内容
    //@Pattern(regexp = "^[,;-_0-9a-zA-Z]$", message = "Please enter correct code")
    private String controllerContent;

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

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getControllerType() {
        return controllerType;
    }

    public void setControllerType(String controllerType) {
        this.controllerType = controllerType;
    }

    public String getControllerContent() {
        return controllerContent;
    }

    public void setControllerContent(String controllerContent) {
        this.controllerContent = controllerContent;
    }
}
