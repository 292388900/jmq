package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;

/**
 * 获取消费位置应答解码器
 */
public class GetConsumeOffsetAckDecoder extends GetConsumeOffsetDecoder {
    @Override
    public int type() {
        return CmdTypes.GET_CONSUMER_OFFSET_ACK;
    }
}