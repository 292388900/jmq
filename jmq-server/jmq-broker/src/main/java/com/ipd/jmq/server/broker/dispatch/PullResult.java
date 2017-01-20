package com.ipd.jmq.server.broker.dispatch;

import com.ipd.jmq.toolkit.buffer.RByteBuffer;

import java.util.List;

/**
 * 拉取的数据
 */
public class PullResult {
    // 主题
    private String topic;
    // 应用
    private String app;
    // 队列
    private short queueId;
    // 数据
    private List<RByteBuffer> buffers;
    // 所有权
    private OwnerShip ownerShip;
    // 派发服务
    private DispatchService dispatchService;

    public PullResult(final String topic, final String app, final short queueId, final List<RByteBuffer> buffers,
            final OwnerShip ownerShip, final DispatchService dispatchService) {
        this.topic = topic;
        this.app = app;
        this.queueId = queueId;
        this.buffers = buffers;
        this.ownerShip = ownerShip;
        this.dispatchService = dispatchService;
    }

    public String getTopic() {
        return topic;
    }

    public String getApp() {
        return app;
    }

    public short getQueueId() {
        return queueId;
    }

    public List<RByteBuffer> getBuffers() {
        return buffers;
    }

    public OwnerShip getOwnerShip() {
        return ownerShip;
    }

    /**
     * 转换成缓冲区数组
     *
     * @return 缓冲区数组
     */
    public RByteBuffer[] toArrays() {
        if (buffers == null) {
            return null;
        }
        return buffers.toArray(new RByteBuffer[buffers.size()]);
    }

    /**
     * 数据条数
     *
     * @return 数据条数
     */
    public int count() {
        if (buffers == null) {
            return 0;
        }
        return buffers.size();
    }

    /**
     * 数据缓冲区大小
     *
     * @return 数据缓冲区大小
     */
    public int size() {
        int len = 0;
        if (buffers != null) {
            for (RByteBuffer buffer : buffers) {
                len += buffer.remaining();
            }
        }
        return len;
    }

    /**
     * 是否为空
     *
     * @return 为空标示
     */
    public boolean isEmpty() {
        return buffers == null || buffers.isEmpty();
    }

    /**
     * 释放资源和队列所有权
     */
    public void release() {
        release(true, true);
    }

    /**
     * 是否资源
     *
     * @param buffer    是否缓冲区
     * @param ownerShip 是否队列所有权
     */
    public void release(final boolean buffer, final boolean ownerShip) {
        if (buffer && buffers != null) {
            for (RByteBuffer buf : buffers) {
                if (buf != null) {
                    buf.release();
                }
            }
        }
        if (ownerShip) {
            dispatchService.cleanExpire(topic, app, queueId, this.ownerShip);
        }

    }
}
