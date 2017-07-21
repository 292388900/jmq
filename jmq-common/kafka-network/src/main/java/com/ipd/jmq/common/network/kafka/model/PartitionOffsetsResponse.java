package com.ipd.jmq.common.network.kafka.model;

import java.util.Set;

/**
 * Created by zhangkepeng on 16-8-4.
 */
public class PartitionOffsetsResponse {

    private short errorCode;
    private Set<Long> offsets;

    public PartitionOffsetsResponse(short errorCode, Set<Long> offsets) {
        this.errorCode = errorCode;
        this.offsets = offsets;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
    }

    public Set<Long> getOffsets() {
        return offsets;
    }

    public void setOffsets(Set<Long> offsets) {
        this.offsets = offsets;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Name: " + this.getClass().getSimpleName());
        stringBuilder.append(",errorCode : " + errorCode);
        for (Long offset : offsets) {
            stringBuilder.append(",offset :" + offset);
        }
        return stringBuilder.toString();
    }
}
