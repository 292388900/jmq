package com.ipd.jmq.common.network.kafka.model;

/**
 * Created by zhangkepeng on 16-8-18.
 */
public class ProducerPartitionStatus {

    private int partition;
    private short errorCode;
    private long offset;

    public ProducerPartitionStatus(short errorCode) {
        this.errorCode = errorCode;
    }

    public ProducerPartitionStatus(short errorCode, long offset) {
        this.errorCode = errorCode;
        this.offset = offset;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }


}
