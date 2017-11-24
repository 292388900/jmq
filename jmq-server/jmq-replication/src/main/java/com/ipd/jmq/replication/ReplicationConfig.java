package com.ipd.jmq.replication;

import com.ipd.jmq.common.network.ServerConfig;

/**
 * Created by guoliang5 on 2016/8/24.
 */
public class ReplicationConfig {
    public static final int SLAVE_LONGPULL_INTERVAL = 10;
    public static final int MAX_RETRY_COUNT = 10;

    public ReplicationConfig() {
    }

    public ReplicationConfig(ServerConfig serverConfig, long insyncSize, int insyncCount, int replicaDeadTimeout) {
        this.serverConfig = serverConfig;
        this.insyncSize = insyncSize;
        this.insyncCount = insyncCount;
        this.replicaDeadTimeout = replicaDeadTimeout;
    }

    public ServerConfig getServerConfig() {
        return serverConfig;
    }

    public void setServerConfig(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    public long getInsyncSize() {
        return insyncSize;
    }

    public void setInsyncSize(long insyncSize) {
        this.insyncSize = insyncSize;
    }

    public int getInsyncCount() {
        return insyncCount;
    }

    public void setInsyncCount(int insyncCount) {
        this.insyncCount = insyncCount;
    }

    public int getReplicaDeadTimeout() {
        return replicaDeadTimeout;
    }

    public void setReplicaDeadTimeout(int replicaDeadTimeout) {
        this.replicaDeadTimeout = replicaDeadTimeout;
    }

    public int getRestartDelay() {
        return restartDelay;
    }

    public void setRestartDelay(int restartDelay) {
        this.restartDelay = restartDelay;
    }

    public int getLongPullTimeout() {
        return longPullTimeout;
    }

    public void setLongPullTimeout(int longPullTimeout) {
        this.longPullTimeout = longPullTimeout;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    private ServerConfig serverConfig;
    private long insyncSize;
    private int insyncCount;
    private int replicaDeadTimeout;
    private int restartDelay;
    private int longPullTimeout = 4000;
    private int blockSize = 4 * 1024 * 1024; // 单次复制最大字节数

}
