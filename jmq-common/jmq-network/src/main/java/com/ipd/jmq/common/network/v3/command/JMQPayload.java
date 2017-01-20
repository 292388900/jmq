package com.ipd.jmq.common.network.v3.command;


import com.ipd.jmq.common.network.Types;
import com.ipd.jmq.common.network.command.Payload;

/**
 * jmq 命令实体.
 *
 * @author lindeqiang
 * @since 2016/8/14 10:59
 */
public abstract class JMQPayload implements Payload, Types {
    /**
     * 校验
     */
    public void validate() {
        //Do nothing
    }

    /**
     * 获取数据包大小
     *
     * @param begin 起始位置
     * @param end   终止位置
     * @return 数据包大小
     */
    protected int getSize(final int begin, final int end) {
        return end - begin - 4;
    }

    /**
     * 预测消息体字节数，便于申请缓冲区
     *
     * @return 消息体字节数
     */
    public int predictionSize() {
        return 0;
    }

}
