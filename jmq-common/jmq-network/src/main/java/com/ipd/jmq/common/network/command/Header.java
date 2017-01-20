package com.ipd.jmq.common.network.command;

import com.ipd.jmq.common.model.Acknowledge;

/**
 * Created by hexiaofeng on 16-6-22.
 */
public interface Header {

    /**
     * 请求ID
     *
     * @return
     */
    int getRequestId();

    /**
     * 设置请求号
     *
     * @param requestId 请求号
     */
    void setRequestId(int requestId);

    /**
     * 获取数据包方向
     *
     * @return 数据包方向
     */
    Direction getDirection();

    /**
     * 设置数据包方向
     *
     * @param direction 方向
     */
    void setDirection(Direction direction);

    /**
     * 获取应答方式
     *
     * @return 应答方式
     */
    Acknowledge getAcknowledge();

    /**
     * 设置应答模式
     *
     * @param acknowledge 应答模式
     */
    void setAcknowledge(Acknowledge acknowledge);
}
