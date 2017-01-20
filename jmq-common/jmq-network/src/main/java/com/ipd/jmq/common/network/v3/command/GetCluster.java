package com.ipd.jmq.common.network.v3.command;

import java.util.*;

/**
 * 获取集群
 */
public class GetCluster extends JMQPayload {
    // 应用
    private String app;
    // 客户端ID
    private String clientId;
    // 客户端所在数据中心
    private byte dataCenter;
    // 主题列表
    private List<String> topics;
    // 参数列表
    private Map<String, String> parameters = new HashMap<String, String>() {
    };

    private byte identity;

    public GetCluster() {
    }

    public GetCluster app(final String app) {
        setApp(app);
        return this;
    }

    public GetCluster clientId(final String clientId) {
        setClientId(clientId);
        return this;
    }

    public GetCluster dataCenter(final byte dataCenter) {
        setDataCenter(dataCenter);
        return this;
    }

    public GetCluster topics(final List<String> topics) {
        setTopics(topics);
        return this;
    }

    public GetCluster topics(final String... topics) {
        if (topics != null) {
            setTopics(Arrays.asList(topics));
        }
        return this;
    }

    public GetCluster identity(final byte identity) {
        setIdentity(identity);
        return this;
    }

    public String getApp() {
        return this.app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public List<String> getTopics() {
        return this.topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public byte getDataCenter() {
        return dataCenter;
    }

    public void setDataCenter(byte dataCenter) {
        this.dataCenter = dataCenter;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public byte getIdentity() {
        return identity;
    }

    public void setIdentity(byte identity) {
        this.identity = identity;
    }

    public int predictionSize() {
        int size = Serializer.getPredictionSize(app, clientId) + 3 + 1;
        if (topics != null) {
            for (String topic : topics) {
                size += Serializer.getPredictionSize(topic);
            }
        }
        return size;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GetCluster{");
        sb.append("app='").append(app).append('\'');
        sb.append(", dataCenter=").append(dataCenter);
        sb.append(", topics=").append(topics);
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

        GetCluster that = (GetCluster) o;

        if (dataCenter != that.dataCenter) {
            return false;
        }
        if (app != null ? !app.equals(that.app) : that.app != null) {
            return false;
        }
        if (topics != null ? !topics.equals(that.topics) : that.topics != null) {
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
        result = 31 * result + (app != null ? app.hashCode() : 0);
        result = 31 * result + (topics != null ? topics.hashCode() : 0);
        result = 31 * result + (int) dataCenter;
        result = 31 * result + (int) identity;
        return result;
    }

    @Override
    public int type() {
        return CmdTypes.GET_CLUSTER;
    }
}