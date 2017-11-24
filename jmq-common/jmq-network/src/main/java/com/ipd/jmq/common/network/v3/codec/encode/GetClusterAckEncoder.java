package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.cluster.BrokerCluster;
import com.ipd.jmq.common.cluster.BrokerGroup;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetClusterAck;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

import java.util.List;

/**
 * 获取集群应答编码器
 */
public class GetClusterAckEncoder implements PayloadEncoder<GetClusterAck> {

    @Override
    public void encode(final GetClusterAck payload, final ByteBuf out) throws Exception {
        payload.validate();

        if (payload.getCacheBody() != null) {
            out.writeBytes(payload.getCacheBody());
        } else {
            out.writeByte(payload.getDataCenter());
            out.writeInt(payload.getInterval());
            out.writeInt(payload.getMaxSize());
            List<BrokerCluster> clusters = payload.getClusters();
            int count = clusters == null ? 0 : clusters.size();
            // 2字节条数
            out.writeShort(count);

            for (int i = 0; i < count; i++) {
                BrokerCluster cluster = clusters.get(i);
                List<BrokerGroup> groups = cluster.getGroups();
                // 1字节主题长度
                Serializer.write(cluster.getTopic(), out);
                // 1字节队列数
                out.writeByte(cluster.getQueues());
                // 1字节分组数量
                int groupCount = groups == null ? 0 : groups.size();
                out.writeByte(groupCount);
                for (int j = 0; j < groupCount; j++) {
                    BrokerGroup group = groups.get(j);
                    // 权限
                    out.writeByte(group.getPermission().ordinal());
                    // Broker数据
                    List<Broker> brokers = group.getBrokers();
                    int brokerCount = brokers == null ? 0 : brokers.size();
                    // 1字节Broker数量
                    out.writeByte(brokerCount);
                    for (int k = 0; k < brokerCount; k++) {
                        Broker broker = brokers.get(k);
                        // 1字节长度字符串
                        Serializer.write(broker.getName(), out);
                        // 1字节长度字符串
                        Serializer.write(broker.getAlias(), out);
                        // 1字节数据中心
                        out.writeByte(broker.getDataCenter());
                        // 1字节权限
                        out.writeByte(broker.getPermission().ordinal());
                    }
                }
            }
            // 兼容原来的协议，权重追加到最后
            for (int i = 0; i < count; i++) {
                BrokerCluster cluster = clusters.get(i);
                List<BrokerGroup> groups = cluster.getGroups();
                // 1字节分组数量
                int groupCount = groups == null ? 0 : groups.size();
                for (int j = 0; j < groupCount; j++) {
                    BrokerGroup group = groups.get(j);
                    // 权限
                    out.writeShort(group.getWeight());
                }
            }
            Serializer.write(payload.getTopicConfigs(), out, 4);
            Serializer.write(payload.getClientConfigs(), out , 4);
            out.writeByte(payload.getIdentity());
        }
    }

    @Override
    public int type() {
        return CmdTypes.GET_CLUSTER_ACK;
    }
}