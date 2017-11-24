package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.cluster.OffsetItem;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.ResetAckOffset;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lindeqiang
 * @since 2016/11/28 16:36
 */
public class ResetAckOffsetDecoder implements PayloadDecoder<ResetAckOffset> {
    @Override
    public ResetAckOffset decode(ResetAckOffset payload, ByteBuf in) throws Exception {
        int count = in.readInt();
        if (count > 0) {
            OffsetItem[] items = new OffsetItem[count];
            for (int i = 0; i < count; i++) {
                OffsetItem item = new OffsetItem();
                item.setTopic(Serializer.readString(in));
                item.setApp(Serializer.readString(in));
                item.setConsumerId(Serializer.readString(in));
                item.setQueueId(in.readShort());
                item.setQueueOffset(in.readLong());
                item.setJournalOffset(in.readLong());
                items[i] = item;
            }
            payload.setItems(items);
        }
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.RESET_ACK_OFFSET;
    }
}
