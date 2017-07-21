package com.ipd.jmq.common.network.kafka.model;


import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.toolkit.time.SystemClock;

/**
 * Created by zhangkepeng on 17-2-21.
 */
public class DelayedResponseKey {

    // 命令
    private Command request;
    // 请求
    private Command response;
    // 通道
    private Transport transport;
    // 结束时间。
    private long expire;
    // 延时相应类型
    private Type type;

    public DelayedResponseKey(Transport transport, Command request, Command response, Type type, long expire) {
        this.transport = transport;
        this.request = request;
        this.response = response;
        this.type = type;
        this.expire = expire + SystemClock.now();
    }

    public Command getRequest() {
        return request;
    }

    public void setRequest(Command request) {
        this.request = request;
    }

    public Command getResponse() {
        return response;
    }

    public void setResponse(Command response) {
        this.response = response;
    }

    public Transport getTransport() {
        return transport;
    }

    public void setTransport(Transport transport) {
        this.transport = transport;
    }

    public long getExpire() {
        return expire;
    }

    public void setExpire(long expire) {
        this.expire = expire;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public enum  Type {
        FETCH,
        OFFSET,
        METADATA,
        COORDINATOR;
    }
}
