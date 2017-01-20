package com.ipd.jmq.common.network.v3.command;

/**
 * 合并消费位置应答
 */
public class GetConsumeOffsetAck extends GetConsumeOffset {
    @Override
    public int type() {
        return CmdTypes.GET_CONSUMER_OFFSET_ACK;
    }
}
