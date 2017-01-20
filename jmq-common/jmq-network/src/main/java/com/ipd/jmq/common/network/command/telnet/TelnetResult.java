package com.ipd.jmq.common.network.command.telnet;

/**
 * Created by zhangkepeng on 16-11-29.
 */
public class TelnetResult {

    // 返回Json字符串
    private String message;

    // 请求状态码
    private short status;

    // 请求类型
    private String type;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public short getStatus() {
        return status;
    }

    public void setStatus(short status) {
        this.status = status;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "{\"message\":\"" + message + "\",\"status\":" + status + ",\"type\":\"" + type + "\"}";
    }
}
