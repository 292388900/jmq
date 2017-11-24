package com.ipd.jmq.common.network.kafka.codec.encode;

import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.command.SyncGroupResponse;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * Created by zhangkepeng on 17-2-10.
 */
public class SyncGroupEncoder implements PayloadEncoder<KafkaRequestOrResponse> {

    @Override
    public void encode(KafkaRequestOrResponse payload, ByteBuf buf) throws Exception {
        if (payload == null || buf == null) {
            return;
        }

        SyncGroupResponse response = (SyncGroupResponse) payload;
        // 开始写位置
        int begin = buf.writerIndex();
        // 长度
        buf.writeInt(0);
        // 响应id
        buf.writeInt(response.getCorrelationId());
        // 错误码
        buf.writeShort(response.getErrorCode());

        ByteBuffer memberState = response.getMemberState();
        int pos = 0;
        if (memberState != null) {
            pos = memberState.position();
            buf.writeInt(memberState.remaining());
            buf.writeBytes(memberState);
            memberState.position(pos);
        }
        buf.writeInt(pos);
        // 写结束位置
        int end = buf.writerIndex();
        // 字节长度
        int length = (end - begin -4);
        // 移到开始位置
        buf.writerIndex(begin);
        // 命令长度
        buf.writeInt(length);
        // 恢复写结束位置
        buf.writerIndex(end);
    }

    @Override
    public short type() {
        return KafkaCommandKeys.SYNC_GROUP;
    }
}
