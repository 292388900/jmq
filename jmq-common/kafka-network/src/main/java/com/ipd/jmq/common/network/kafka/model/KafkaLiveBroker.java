package com.ipd.jmq.common.network.kafka.model;

import java.io.Serializable;

/**
 * Created by zhangkepeng on 16-8-16.
 *
 * zookeeper序列化类
 */
public class KafkaLiveBroker implements Serializable{
    private int jmx_port;
    private String timestamp;
    private String host;
    private int version;
    private int port;

    public int getJmx_port() {
        return jmx_port;
    }

    public void setJmx_port(int jmx_port) {
        this.jmx_port = jmx_port;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
