package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetCluster;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * 获取集群解码器
 */
public class GetClusterDecoder implements PayloadDecoder<GetCluster> {

    @Override
    public GetCluster decode(final GetCluster payload, final ByteBuf in) throws Exception {
        payload.setApp(Serializer.readString(in));
        payload.setClientId(Serializer.readString(in));
        payload.setDataCenter(in.readByte());
        int count = in.readUnsignedShort();
        List<String> topics = new ArrayList<String>();
        for (int i = 0; i < count; i++) {
            // 1字节主题长度
            topics.add(Serializer.readString(in));
        }
        payload.setTopics(topics);

        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.GET_CLUSTER;
    }
}