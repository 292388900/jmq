package com.ipd.jmq.server.store;

import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.message.MessageLocation;

import java.util.List;

/**
 * 存储结果
 */
public class PutResult {
    // 代码
    private JMQCode code = JMQCode.SUCCESS;
    // 位置
    private List<MessageLocation> locations;
    // 存储时间
    private long storeTime;
    // 水位
    private long waterMark;
    // 复制状态
    private JMQCode replicationCode = JMQCode.SUCCESS;

    public JMQCode getCode() {
        return code;
    }

    public void setCode(JMQCode code) {
        this.code = code;
    }

    public List<MessageLocation> getLocation() {
        return this.locations;
    }

    public void setLocation(List<MessageLocation> locations) {
        this.locations = locations;
    }

    public long getStoreTime() {
        return this.storeTime;
    }

    public void setStoreTime(long storeTime) {
        this.storeTime = storeTime;
    }

    public JMQCode getReplicationCode() {
        return replicationCode;
    }

    public void setReplicationCode(JMQCode replicationCode) {
        this.replicationCode = replicationCode;
    }

    public long getWaterMark() {
        return waterMark;
    }

    public void setWaterMark(long waterMark) {
        this.waterMark = waterMark;
    }

    public PutResult() {

    }

    /**
     * 构造函数
     *
     * @param code      代码
     * @param locations 位置
     */
    public PutResult(JMQCode code, List<MessageLocation> locations) {
        this.code = code;
        this.locations = locations;
    }

}