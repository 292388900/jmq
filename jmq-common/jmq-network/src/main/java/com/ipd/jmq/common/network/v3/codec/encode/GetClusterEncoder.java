package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetCluster;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

import java.util.List;

/**
 * 获取集群编码器
 */
public class GetClusterEncoder implements PayloadEncoder<GetCluster> {

    @Override
    public void encode(final GetCluster payload, final ByteBuf out) throws Exception {
        payload.validate();
        // 1字节应用长度
        Serializer.write(payload.getApp(), out);
        // 1字节客户端ID
        Serializer.write(payload.getClientId(), out);
        // 1字节数据中心
        out.writeByte(payload.getDataCenter());
        // 2字节条数
        List<String> topics = payload.getTopics();
        out.writeShort(topics == null ? 0 : topics.size());
        if (topics != null && !topics.isEmpty()) {
            for (String topic : topics) {
                // 1字节主题长度
                Serializer.write(topic, out);
            }
        }
        Serializer.write(payload.getParameters(), out);
    }

    @Override
    public int type() {
        return CmdTypes.GET_CLUSTER;
    }
}