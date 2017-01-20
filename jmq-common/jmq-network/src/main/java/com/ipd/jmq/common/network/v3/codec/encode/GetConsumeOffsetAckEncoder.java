package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;

public class GetConsumeOffsetAckEncoder extends GetConsumeOffsetEncoder {
    @Override
    public int type() {
        return CmdTypes.GET_CONSUMER_OFFSET_ACK;
    }
}