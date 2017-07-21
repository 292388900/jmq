package com.ipd.jmq.common.network.kafka.codec.encode;

import com.ipd.jmq.common.network.kafka.command.*;
import com.ipd.jmq.common.network.kafka.model.PartitionOffsetsResponse;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import com.ipd.jmq.common.network.v3.command.Direction;
import io.netty.buffer.ByteBuf;

import java.util.Set;

/**
 * Created by zhangkepeng on 17-2-16.
 */
public class OffsetQueryEncoder implements PayloadEncoder<KafkaRequestOrResponse> {

    @Override
    public void encode(KafkaRequestOrResponse payload, ByteBuf buf) throws Exception {
        if (payload == null || buf == null) {
            return;
        }

        // 记录开始写位置
        int begin = buf.writerIndex();
        // 四字节长度
        buf.writeInt(0);
        if (payload instanceof OffsetQueryRequest) {
            OffsetQueryRequest request = (OffsetQueryRequest) payload;
            buf.writeShort(request.type());
            buf.writeInt(Direction.REQUEST.getValue());
            buf.writeShort(request.getVersion());
            buf.writeInt(request.getCorrelationId());
            CommandUtils.writeShortString(buf, request.getClientId());
            CommandUtils.writeShortString(buf, request.getTopic());
            buf.writeInt(request.getPartition());
            buf.writeLong(request.getTimestamp());
            buf.writeInt(request.getMaxNumOffsets());
        } else {
            OffsetQueryResponse response = (OffsetQueryResponse) payload;
            buf.writeShort(response.type());
            buf.writeInt(Direction.RESPONSE.getValue());
            buf.writeInt(response.getCorrelationId());
            PartitionOffsetsResponse partitionOffsetsResponse = response.getPartitionOffsetsResponse();
            // 错误码
            buf.writeShort(partitionOffsetsResponse.getErrorCode());
            Set<Long> offsets = partitionOffsetsResponse.getOffsets();
            // 符合条件的偏移量个数
            buf.writeInt(offsets.size());
            // 写入偏移量
            for (long offset : offsets) {
                buf.writeLong(offset);
            }
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
        return KafkaCommandKeys.OFFSET_QUERY;
    }
}
