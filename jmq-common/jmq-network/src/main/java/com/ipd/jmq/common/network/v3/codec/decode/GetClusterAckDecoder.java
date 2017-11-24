package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.cluster.*;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetClusterAck;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * 获取集群应答解码器
 */
public class GetClusterAckDecoder implements PayloadDecoder<GetClusterAck> {

    @Override
    public GetClusterAck decode(final GetClusterAck payload, final ByteBuf in) throws Exception {
        payload.setDataCenter(in.readByte());
        payload.setInterval(in.readInt());
        payload.setMaxSize(in.readInt());
        List<BrokerCluster> clusters = new ArrayList<BrokerCluster>();
        payload.setClusters(clusters);
        // 2字节条数
        int length = in.readUnsignedShort();
        if (length < 1) {
            return null;
        }
        for (int i = 0; i < length; i++) {
            // 1字节主题长度
            String topic = Serializer.readString(in);
            // 1字节队列数
            short queue = in.readUnsignedByte();
            List<BrokerGroup> groups = new ArrayList<BrokerGroup>();
            BrokerCluster cluster = new BrokerCluster(topic, groups, queue);
            // 1字节分组数量
            int groupCount = in.readUnsignedByte();
            for (int j = 0; j < groupCount; j++) {
                BrokerGroup group = new BrokerGroup();
                group.setPermission(Permission.valueOf(in.readByte()));
                int brokerCount = in.readUnsignedByte();
                for (int k = 0; k < brokerCount; k++) {
                    Broker broker = new Broker(Serializer.readString(in));
                    broker.setAlias(Serializer.readString(in));
                    broker.setDataCenter(in.readByte());
                    broker.setPermission(Permission.valueOf(in.readByte()));
                    group.addBroker(broker);
                    if (group.getGroup() == null && broker.getGroup() != null) {
                        group.setGroup(broker.getGroup());
                    }
                }
                cluster.addGroup(group);
            }
            clusters.add(cluster);
        }
        // 读取权重
        for (int i = 0; i < length; i++) {
            BrokerCluster cluster = clusters.get(i);
            List<BrokerGroup> groups = cluster.getGroups();
            int groupCount = groups.size();
            for (int j = 0; j < groupCount; j++) {
                BrokerGroup group = groups.get(j);
                group.setWeight(in.readShort());
            }
        }
        if (in.isReadable()) {
            payload.setTopicConfigs(Serializer.readString(in, 4));
        }
        if (in.isReadable()) {
            payload.setClientConfigs(Serializer.readString(in, 4));
        }
        payload.setIdentity(in.readByte());

        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.GET_CLUSTER_ACK;
    }
}