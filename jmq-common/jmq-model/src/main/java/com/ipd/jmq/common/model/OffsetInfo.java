package com.ipd.jmq.common.model;

import java.io.Serializable;

/**
 * 偏移量信息
 *
 * Created by zhangkepeng on 15-11-13.
 */
public class OffsetInfo implements Serializable {
    private static final long serialVersionUID = 3704812359800945897L;
    // 服务端最小消费时间戳
    private long minOffsetForDate;
    // 服务端最小消费位置
    private long minOffsetForValue;
    // 服务端确认消费时间戳
    private long ackOffsetForDate;
    // 服务端确认消费位置
    private long ackOffsetForValue;
    // 服务端最大消费时间戳
    private long maxOffsetForDate;
    // 服务端最大消费位置
    private long maxOffsetForValue;

    public long getMinOffsetForDate() {
        return minOffsetForDate;
    }

    public long getMinOffsetForValue() {
        return minOffsetForValue;
    }

    public long getAckOffsetForValue() {
        return ackOffsetForValue;
    }

    public long getAckOffsetForDate() {
        return ackOffsetForDate;
    }

    public long getMaxOffsetForDate() {
        return maxOffsetForDate;
    }

    public long getMaxOffsetForValue() {
        return maxOffsetForValue;
    }

    public void setMinOffsetForDate(long minOffsetForDate) {
        this.minOffsetForDate = minOffsetForDate;
    }

    public void setMinOffsetForValue(long minOffsetForValue) {
        this.minOffsetForValue = minOffsetForValue;
    }

    public void setAckOffsetForDate(long ackOffsetForDate) {
        this.ackOffsetForDate = ackOffsetForDate;
    }

    public void setAckOffsetForValue(long ackOffsetForValue) {
        this.ackOffsetForValue = ackOffsetForValue;
    }

    public void setMaxOffsetForDate(long maxOffsetForDate) {
        this.maxOffsetForDate = maxOffsetForDate;
    }

    public void setMaxOffsetForValue(long maxOffsetForValue) {
        this.maxOffsetForValue = maxOffsetForValue;
    }
}
