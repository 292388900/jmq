package com.ipd.jmq.common.network.kafka.codec.encode;

import com.ipd.jmq.common.network.kafka.command.GroupCoordinatorResponse;
import com.ipd.jmq.common.network.kafka.model.KafkaBroker;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import io.netty.buffer.ByteBuf;

/**
 * Created by zhangkepeng on 16-8-16.
 */
public class GroupCoordinatorEncoder implements PayloadEncoder<KafkaRequestOrResponse> {


    @Override
    public void encode(KafkaRequestOrResponse payload, ByteBuf buf) throws Exception {
        if (payload == null || buf == null) {
            return;
        }

        GroupCoordinatorResponse response = (GroupCoordinatorResponse) payload;
        // 开始写位置
        int begin = buf.writerIndex();
        // 长度
        buf.writeInt(0);
        // 响应id
        buf.writeInt(response.getCorrelationId());
        // 错误码
        buf.writeShort(response.getErrorCode());

        KafkaBroker kafkaBroker = response.getKafkaBroker();
        buf.writeInt(kafkaBroker.getId());
        CommandUtils.writeShortString(buf, kafkaBroker.getHost());
        buf.writeInt(kafkaBroker.getPort());
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
        return KafkaCommandKeys.GROUP_COORDINATOR;
    }
}
