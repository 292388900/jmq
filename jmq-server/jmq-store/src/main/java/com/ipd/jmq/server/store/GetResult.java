package com.ipd.jmq.server.store;

import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.toolkit.buffer.RByteBuffer;
import com.ipd.jmq.toolkit.ref.Reference;

import java.util.List;


/**
 * 查询结果
 */
public class GetResult implements Reference{

    // 代码
    private JMQCode code = JMQCode.SE_IO_ERROR;
    // 缓冲区
    private List<RByteBuffer> buffers;
    // 数据条数
    private int length;
    // 第一条数据队列偏移量
    private long minQueueOffset = -1L;
    // 最后一条数据队列偏移量
    private long maxQueueOffset = -1L;
    // 下一次拉取数据队列偏移量
    private long nextOffset = -1L;
    // 队列最小时间戳，用于延迟消费辅助判断
    private MinTimeOffset timeOffset;
    // 确认位置
    private long ackOffset;

    public JMQCode getCode() {
        return code;
    }

    public void setCode(JMQCode code) {
        this.code = code;
    }

    public List<RByteBuffer> getBuffers() {
        return this.buffers;
    }

    public void setBuffers(List<RByteBuffer> buffers) {
        this.buffers = buffers;
        if (buffers == null) {
            length = 0;
        } else {
            length = buffers.size();
        }
    }

    public int getLength() {
        return this.length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public long getMinQueueOffset() {
        return this.minQueueOffset;
    }

    public void setMinQueueOffset(long minQueueOffset) {
        this.minQueueOffset = minQueueOffset;
    }

    public long getMaxQueueOffset() {
        return this.maxQueueOffset;
    }

    public void setMaxQueueOffset(long maxQueueOffset) {
        this.maxQueueOffset = maxQueueOffset;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }

    public MinTimeOffset getTimeOffset() {
        return timeOffset;
    }

    public void setTimeOffset(MinTimeOffset timeOffset) {
        this.timeOffset = timeOffset;
    }

    public long getAckOffset() {
        return ackOffset;
    }

    public void setAckOffset(long ackOffset) {
        this.ackOffset = ackOffset;
    }

    @Override
    public void acquire() {

    }

    @Override
    public boolean release() {
        if (buffers != null) {
            for (RByteBuffer buffer : buffers) {
                    buffer.release();
            }
        }
        return true;
    }

    @Override
    public long references() {
        return 0;
    }

    /**
     * 数据缓冲区大小
     *
     * @return 数据缓冲区大小
     */
    public int messageBodyLength() {
        int len = 0;
        if (buffers != null) {
            for (RByteBuffer buffer : buffers) {
                len += buffer.remaining();
            }
        }
        return len;
    }
}