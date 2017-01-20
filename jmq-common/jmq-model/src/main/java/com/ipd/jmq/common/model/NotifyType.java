package com.ipd.jmq.common.model;

/**
 * 通知类型
 * Created by hexiaofeng on 16-8-23.
 */
public enum NotifyType implements EnumBits {
    EMAIL(1, "邮件", "text/html;charset=utf-8"),
    SMS(2, "短信", "text/plain;charset=utf-8");

    private int value;
    private String description;
    private String contentType;

    NotifyType(int value, String description, String contentType) {
        this.value = value;
        this.description = description;
        this.contentType = contentType;
    }

    public int value() {
        return value;
    }

    public String description() {
        return description;
    }

    public String getContentType() {
        return contentType;
    }

}
