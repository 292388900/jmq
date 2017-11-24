package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.cluster.OffsetItem;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.ResetAckOffset;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 重置消费位置编码器.
 *
 * @author lindeqiang
 * @since 2016/11/28 16:12
 */
public class ResetAckOffsetEncoder implements PayloadEncoder<ResetAckOffset> {
    @Override
    public void encode(ResetAckOffset payload, ByteBuf out) throws Exception {
        payload.validate();
        int size = payload.getItems().length;
        out.writeInt(size);
        if (size > 0) {
            for (OffsetItem item : payload.getItems()) {
                Serializer.write(item.getTopic(), out);
                Serializer.write(item.getApp(), out);
                Serializer.write(item.getConsumerId(), out);
                out.writeShort(item.getQueueId());
                out.writeLong(item.getQueueOffset());
                out.writeLong(item.getJournalOffset());
            }
        }
    }

    @Override
    public int type() {
        return CmdTypes.RESET_ACK_OFFSET;
    }
}
