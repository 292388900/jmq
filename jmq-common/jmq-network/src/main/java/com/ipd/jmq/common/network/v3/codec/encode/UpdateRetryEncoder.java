package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.command.UpdateRetry;
import io.netty.buffer.ByteBuf;

/**
 * 更新重试编码器
 */
public class UpdateRetryEncoder implements PayloadEncoder<UpdateRetry> {

    @Override
    public void encode(final UpdateRetry payload, final ByteBuf out) throws Exception {
        payload.validate();
        Serializer.write(payload.getTopic(), out);
        Serializer.write(payload.getApp(), out);
        out.writeByte(payload.getUpdateType());
        long[] messages = payload.getMessages();
        int count = messages == null ? 0 : messages.length;
        out.writeShort(count);
        if (count > 0) {
            for (int i = 0; i < messages.length; i++) {
                out.writeLong(messages[i]);
            }
        }
    }

    @Override
    public int type() {
        return CmdTypes.UPDATE_RETRY;
    }
}