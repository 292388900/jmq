package com.ipd.jmq.common.network.kafka.command;

import java.util.Map;

/**
 * Created by zhangkepeng on 16-7-27.
 */
public class FetchRequest implements KafkaRequestOrResponse {

    private short version;
    private int correlationId;
    private String clientId;

    private int replicaId;
    private int maxWait;
    private int minBytes;
    private Map<String, Map<Integer, PartitionFetchInfo>> requestInfo;
    private int numPartitions;

    public short getVersion() {
        return version;
    }

    public void setVersion(short version) {
        this.version = version;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public int getMinBytes() {
        return minBytes;
    }

    public void setMinBytes(int minBytes) {
        this.minBytes = minBytes;
    }

    public Map<String, Map<Integer, PartitionFetchInfo>> getRequestInfo() {
        return requestInfo;
    }

    public void setRequestInfo(Map<String, Map<Integer, PartitionFetchInfo>> requestInfo) {
        this.requestInfo = requestInfo;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public int getMaxWait() {
        return maxWait;
    }

    public void setMaxWait(int maxWait) {
        this.maxWait = maxWait;
    }

    public int getReplicaId() {
        return replicaId;
    }

    public void setReplicaId(int replicaId) {
        this.replicaId = replicaId;
    }

    @Override
    public int type() {
        return KafkaCommandKeys.FETCH;
    }

    @Override
    public String toString() {
        return describe(true);
    }

    public String describe(boolean details) {
        StringBuilder fetchRequest = new StringBuilder();
        fetchRequest.append("Name: " + this.getClass().getSimpleName());
        fetchRequest.append("; ReplicaId: " + replicaId);
        fetchRequest.append("; MaxWait: " + maxWait + " ms");
        fetchRequest.append("; MinBytes: " + minBytes + " bytes");
        return fetchRequest.toString();
    }

    public class PartitionFetchInfo {

        private long offset;
        private int fetchSize;

        public PartitionFetchInfo(long offst, int fetchSize) {
            this.offset = offst;
            this.fetchSize = fetchSize;
        }

        public long getOffset() {
            return offset;
        }

        public int getFetchSize() {
            return fetchSize;
        }
    }
}