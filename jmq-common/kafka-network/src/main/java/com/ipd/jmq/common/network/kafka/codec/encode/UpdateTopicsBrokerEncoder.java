package com.ipd.jmq.common.network.kafka.codec.encode;

import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaRequestOrResponse;
import com.ipd.jmq.common.network.kafka.command.UpdateTopicsBrokerRequest;
import com.ipd.jmq.common.network.kafka.command.UpdateTopicsBrokerResponse;
import com.ipd.jmq.common.network.kafka.utils.CommandUtils;
import com.ipd.jmq.common.network.v3.command.Direction;
import io.netty.buffer.ByteBuf;

/**
 * Created by zhangkepeng on 16-9-6.
 */
public class UpdateTopicsBrokerEncoder implements PayloadEncoder<KafkaRequestOrResponse> {

    @Override
    public void encode(KafkaRequestOrResponse payload, ByteBuf buf) throws Exception {
        if (payload == null || buf == null) {
            return;
        }

        // 记录开始写位置
        int begin = buf.writerIndex();
        // 四字节长度
        buf.writeInt(0);
        if (payload instanceof UpdateTopicsBrokerRequest) {
            UpdateTopicsBrokerRequest request = (UpdateTopicsBrokerRequest) payload;
            buf.writeShort(request.type());
            buf.writeInt(Direction.REQUEST.getValue());
            buf.writeShort(request.getVersion());
            buf.writeInt(request.getCorrelationId());
            CommandUtils.writeShortString(buf, request.getClientId());
            CommandUtils.writeShortString(buf, request.getTopic());
            buf.writeInt(request.getPartition());
            buf.writeInt(request.getTopicsBrokerType());
            buf.writeInt(request.getLastBrokerId());
            buf.writeInt(request.getControllerEpoch());
        } else {
            UpdateTopicsBrokerResponse response = (UpdateTopicsBrokerResponse) payload;
            buf.writeShort(response.type());
            buf.writeInt(Direction.RESPONSE.getValue());
            buf.writeInt(response.getCorrelationId());
            CommandUtils.writeShortString(buf, response.getTopic());
            buf.writeShort(response.getErrorCode());
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
        return KafkaCommandKeys.UPDATE_TOPICS_BROKER;
    }
}
