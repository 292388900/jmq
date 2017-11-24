package com.ipd.jmq.common.network;

import com.ipd.jmq.toolkit.os.Systems;

/**
 * 客户端传输配置
 */
public class ClientConfig extends Config {
    // 连接超时(毫秒)
    private int connectionTimeout = 5000;

    public ClientConfig() {
        super();
        workerThreads = 4;
        callbackThreads = Systems.getCores();
        selectorThreads = 1;
        maxIdleTime = 120 * 1000;
        maxOneway = 256;
        maxAsync = 128;
        setEpoll(true);
    }

    public int getConnectionTimeout() {
        return this.connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        if (connectionTimeout > 0) {
            this.connectionTimeout = connectionTimeout;
        }
    }

    public static class Builder extends Config.Builder<ClientConfig, Builder> {
        public Builder() {
            config = new ClientConfig();
        }

        public Builder connectionTimeout(final int connectionTimeout) {
            config.setConnectionTimeout(connectionTimeout);
            return this;
        }

        public static Builder create() {
            return new Builder();
        }
    }

}