package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.cluster.BrokerCluster;
import com.ipd.jmq.common.cluster.BrokerGroup;

import java.util.Arrays;
import java.util.List;

/**
 * 获取集群应答
 */
public class GetClusterAck extends JMQPayload {
    // 集群应答
    protected List<BrokerCluster> clusters;
    // 客户端所在数据中心
    protected byte dataCenter;
    // 单次调用，生产消息的消息体合计最大大小
    protected int maxSize;
    // 更新集群间隔时间
    protected int interval;
    // TopicConfig序列化字符串
    protected String topicConfigs;
    // all dynamic changed client configs
    protected String clientConfigs;
    // identity for changes
    private byte identity = 0;

    protected byte[] cacheBody;

    public GetClusterAck() {
    }

    public GetClusterAck dataCenter(byte dataCenter) {
        setDataCenter(dataCenter);
        return this;
    }

    public GetClusterAck clusters(final List<BrokerCluster> clusters) {
        setClusters(clusters);
        return this;
    }

    public GetClusterAck clusters(final BrokerCluster... clusters) {
        if (clusters != null) {
            setClusters(Arrays.asList(clusters));
        }
        return this;
    }

    public GetClusterAck interval(final int interval) {
        setInterval(interval);
        return this;
    }

    public GetClusterAck maxSize(final int maxSize) {
        setMaxSize(maxSize);
        return this;
    }

    public GetClusterAck allTopicConfigStrings(final String allTopicConfigStrings) {
        setTopicConfigs(allTopicConfigStrings);
        return this;
    }

    public GetClusterAck allClientConfigStrings(final String allClientConfigStrings) {
        setClientConfigs(allClientConfigStrings);
        return this;
    }

    public GetClusterAck identity(final byte identity) {
        setIdentity(identity);
        return this;
    }

    public List<BrokerCluster> getClusters() {
        return this.clusters;
    }

    public void setClusters(List<BrokerCluster> clusters) {
        this.clusters = clusters;
    }

    public byte getDataCenter() {
        return dataCenter;
    }

    public void setDataCenter(byte dataCenter) {
        this.dataCenter = dataCenter;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public String getTopicConfigs() {
        return topicConfigs;
    }

    public void setTopicConfigs(String topicConfigs) {
        this.topicConfigs = topicConfigs;
    }

    public String getClientConfigs() {
        return clientConfigs;
    }

    public void setClientConfigs(String clientConfigs) {
        this.clientConfigs = clientConfigs;
    }

    public byte getIdentity() {
        return identity;
    }

    public void setIdentity(byte identity) {
        this.identity = identity;
    }

    @Override
    public int type() {
        return CmdTypes.GET_CLUSTER_ACK;
    }

    public byte[] getCacheBody() {
        return cacheBody;
    }

    public void setCacheBody(byte[] cacheBody) {
        this.cacheBody = cacheBody;
    }

    public int predictionSize() {
        int size = 11 + 1;
        if (clusters != null) {
            for (int i = 0; i < clusters.size(); i++) {
                BrokerCluster cluster = clusters.get(i);
                List<BrokerGroup> groups = cluster.getGroups();
                size += Serializer.getPredictionSize(cluster.getTopic()) + 2;
                if (groups != null) {
                    for (int j = 0; j < groups.size(); j++) {
                        BrokerGroup group = groups.get(j);
                        size += 2;
                        // weight
                        size += 2;
                        // Broker数据
                        List<Broker> brokers = group.getBrokers();
                        if (brokers != null) {
                            for (int k = 0; k < brokers.size(); k++) {
                                Broker broker = brokers.get(k);
                                size += Serializer.getPredictionSize(broker.getName(), broker.getAlias()) + 2;
                            }
                        }
                    }
                }
            }
        }
        size += Serializer.getPredictionSize(topicConfigs);
        size += Serializer.getPredictionSize(clientConfigs);
        return size;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GetClusterAck{");
        sb.append("clusters=").append(clusters);
        sb.append(", dataCenter=").append(dataCenter);
        sb.append(", maxSize=").append(maxSize);
        sb.append(", interval=").append(interval);
        sb.append(", topicConfigs=").append(topicConfigs);
        sb.append(", clientConfigs=").append(clientConfigs);
        sb.append(", identity=").append(identity);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        GetClusterAck that = (GetClusterAck) o;

        if (dataCenter != that.dataCenter) {
            return false;
        }
        if (interval != that.interval) {
            return false;
        }
        if (maxSize != that.maxSize) {
            return false;
        }
        if (clusters != null ? !clusters.equals(that.clusters) : that.clusters != null) {
            return false;
        }
        if (!topicConfigs.equals(that.topicConfigs)) {
            return false;
        }

        if (!clientConfigs.equals(that.clientConfigs)) {
            return false;
        }
        if (identity != that.identity) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (clusters != null ? clusters.hashCode() : 0);
        result = 31 * result + (int) dataCenter;
        result = 31 * result + maxSize;
        result = 31 * result + interval;
        result = 31 * result + (int) identity;
        return result;
    }
}