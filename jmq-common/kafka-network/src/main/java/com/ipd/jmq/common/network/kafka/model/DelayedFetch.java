package com.ipd.jmq.common.network.kafka.model;

import com.ipd.jmq.common.network.kafka.command.FetchRequest;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.toolkit.time.SystemClock;

import java.util.Set;

/**
 * Created by zhangkepeng on 16-8-29.
 */
public class DelayedFetch {

    // 主题和
    private Set<TopicAndPartition> topicAndPartitions;
    // 命令
    private Command command;
    // 请求
    private FetchRequest payload;
    // 通道
    private Transport transport;
    // 结束时间。
    private long expire;

    public DelayedFetch(Set<TopicAndPartition> topicAndPartitions, Command command, Transport transport) {
        this.topicAndPartitions = topicAndPartitions;
        this.payload = (FetchRequest)command.getPayload();
        this.command = command;
        this.transport = transport;
        this.expire = SystemClock.now() + payload.getMaxWait();
    }

    public long getExpire() {
        return expire;
    }

    public void setExpire(long expire) {
        this.expire = expire;
    }

    public Set<TopicAndPartition> getTopicAndPartitions() {
        return topicAndPartitions;
    }

    public void setTopicAndPartitions(Set<TopicAndPartition> topicAndPartitions) {
        this.topicAndPartitions = topicAndPartitions;
    }

    public Command getCommand() {
        return command;
    }

    public void setCommand(Command command) {
        this.command = command;
    }

    public FetchRequest getPayload() {
        return payload;
    }

    public void setPayload(FetchRequest payload) {
        this.payload = payload;
    }

    public Transport getTransport() {
        return transport;
    }

    public void setTransport(Transport transport) {
        this.transport = transport;
    }
}
