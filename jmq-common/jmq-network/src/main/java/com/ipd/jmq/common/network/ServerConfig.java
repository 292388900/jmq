package com.ipd.jmq.common.network;

import com.ipd.jmq.toolkit.os.Systems;

/**
 * 服务端传输配置
 */
public class ServerConfig extends Config {
    // IP地址
    private String ip;
    // 端口
    private int port = 50088;
    // 连接请求最大队列长度，如果队列满时收到连接指示，则拒绝该连接。
    private int backlog = 65536;

    public ServerConfig() {
        super();
        // 目前线上机器默认24核
        workerThreads = Systems.getCores();
        selectorThreads = 8;
        maxOneway = 24;
        maxAsync = 48;
        maxIdleTime = 120 * 1000;
        setEpoll(true);
    }

    public ServerConfig(ServerConfig config, int port) {
        super(config);
        setIp(config.getIp());
        setPort(port);
        setBacklog(config.getBacklog());
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return this.port;
    }

    public void setPort(int port) {
        if (port > 0 && port <= 65535) {
            this.port = port;
        }
    }

    public int getBacklog() {
        return this.backlog;
    }

    public void setBacklog(int backlog) {
        if (backlog > 0) {
            this.backlog = backlog;
        }
    }

    public static class Builder extends Config.Builder<ServerConfig, Builder> {

        public Builder() {
            config = new ServerConfig();
        }

        public Builder backlog(final int backlog) {
            config.setBacklog(backlog);
            return this;
        }

        public Builder ip(final String ip) {
            config.setIp(ip);
            return this;
        }

        public Builder port(final int port) {
            config.setPort(port);
            return this;
        }

        public static Builder create() {
            return new Builder();
        }
    }

}