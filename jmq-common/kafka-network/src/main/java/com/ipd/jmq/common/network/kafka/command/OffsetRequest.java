package com.ipd.jmq.common.network.kafka.command;

import java.util.Map;

/**
 * Created by zhangkepeng on 16-7-28.
 */
public class OffsetRequest implements KafkaRequestOrResponse {

    private final String smallestTimeString = "smallest";
    private final String largestTimeString = "largest";
    public static final long LATEST_TIME = -1L;
    public static final long EARLIEST_TIME = -2L;

    private short version;
    private int correlationId;
    private String clientId;

    private Map<String, Map<Integer, PartitionOffsetRequestInfo>> offsetRequestMap;
    private int replicaId;

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    public short getVersion() {
        return version;
    }

    public void setVersion(short version) {
        this.version = version;
    }

    public int getReplicaId() {
        return replicaId;
    }

    public void setReplicaId(int replicaId) {
        this.replicaId = replicaId;
    }

    public Map<String, Map<Integer, PartitionOffsetRequestInfo>> getOffsetRequestMap() {
        return offsetRequestMap;
    }

    public void setOffsetRequestMap(Map<String, Map<Integer, PartitionOffsetRequestInfo>> offsetRequestMap) {
        this.offsetRequestMap = offsetRequestMap;
    }

    @Override
    public int type() {
        return KafkaCommandKeys.LIST_OFFSETS;
    }

    @Override
    public String toString() {
        return describe();
    }

    public String describe() {
        StringBuilder offsetRequest = new StringBuilder();
        offsetRequest.append("Name: " + this.getClass().getSimpleName());
        offsetRequest.append("; ReplicaId: " + replicaId);
        return offsetRequest.toString();
    }

    public class PartitionOffsetRequestInfo {

        private long time;
        private int maxNumOffsets;

        public PartitionOffsetRequestInfo(long time, int maxNumOffsets) {
            this.time = time;
            this.maxNumOffsets = maxNumOffsets;
        }

        public long getTime() {
            return time;
        }

        public int getMaxNumOffsets() {
            return maxNumOffsets;
        }
    }
}
