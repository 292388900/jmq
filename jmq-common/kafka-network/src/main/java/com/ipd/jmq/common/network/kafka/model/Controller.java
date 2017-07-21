package com.ipd.jmq.common.network.kafka.model;

import java.io.Serializable;

/**
 * Created by zhangkepeng on 16-8-29.
 */
public class Controller implements Serializable{
    // 版本
    private int version = 1;
    // 分片ID
    private int brokerid;
    // 时间戳
    private long timestamp;

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getBrokerid() {
        return brokerid;
    }

    public void setBrokerid(int brokerid) {
        this.brokerid = brokerid;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
