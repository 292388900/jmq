package com.ipd.jmq.common.network.kafka.model;

import com.ipd.jmq.common.network.kafka.exception.ErrorCode;
import com.ipd.jmq.common.network.kafka.message.ByteBufferMessageSet;

/**
 * Created by zhangkepeng on 16-8-17.
 *
 */
public class FetchResponsePartitionData {

    private short error = ErrorCode.NO_ERROR;
    private long hw = -1L;
    private ByteBufferMessageSet byteBufferMessageSet;

    public FetchResponsePartitionData() {

    }

    public FetchResponsePartitionData(short error, long hw, ByteBufferMessageSet messageSet) {
        this.error = error;
        this.hw = hw;
        this.byteBufferMessageSet = messageSet;
    }

    public short getError() {
        return error;
    }

    public void setError(short error) {
        this.error = error;
    }

    public long getHw() {
        return hw;
    }

    public void setHw(long hw) {
        this.hw = hw;
    }

    public ByteBufferMessageSet getByteBufferMessageSet() {
        return byteBufferMessageSet;
    }

    public void setByteBufferMessageSet(ByteBufferMessageSet byteBufferMessageSet) {
        this.byteBufferMessageSet = byteBufferMessageSet;
    }
}
