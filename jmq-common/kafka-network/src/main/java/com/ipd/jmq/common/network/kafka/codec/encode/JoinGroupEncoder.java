package com.ipd.jmq.common.network.kafka.codec.encode;

import com.ipd.jmq.common.network.kafka.command.JoinGroupResponse;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by zhangkepeng on 17-2-9.
 */
public class JoinGroupEncoder implements PayloadEncoder<KafkaRequestOrResponse> {

    @Override
    public void encode(KafkaRequestOrResponse payload, ByteBuf buf) throws Exception {
        if (payload == null || buf == null) {
            return;
        }

        JoinGroupResponse response = (JoinGroupResponse) payload;
        // 开始写位置
        int begin = buf.writerIndex();
        // 长度
        buf.writeInt(0);
        // 响应id
        buf.writeInt(response.getCorrelationId());
        // 错误码
        buf.writeShort(response.getErrorCode());

        buf.writeInt(response.getGenerationId());
        CommandUtils.writeShortString(buf, response.getGroupProtocol());
        CommandUtils.writeShortString(buf, response.getLeaderId());
        CommandUtils.writeShortString(buf, response.getMemberId());
        Map<String, ByteBuffer> members = response.getMembers();
        if (members != null) {
            int size = members.size();
            buf.writeInt(size);
            for (Map.Entry<String, ByteBuffer> entry : members.entrySet()) {
                CommandUtils.writeShortString(buf, entry.getKey());
                ByteBuffer arg = entry.getValue();
                int pos = arg.position();
                buf.writeInt(arg.remaining());
                buf.writeBytes(arg);
                arg.position(pos);
            }
        } else {
            buf.writeInt(0);
        }

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
        return KafkaCommandKeys.JOIN_GROUP;
    }
}
