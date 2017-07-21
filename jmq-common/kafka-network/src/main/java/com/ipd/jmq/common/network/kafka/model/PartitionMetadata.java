package com.ipd.jmq.common.network.kafka.model;

import java.util.Set;

/**
 * Created by zhangkepeng on 16-8-2.
 */
public class PartitionMetadata {
    private int partitionId;
    private KafkaBroker leader;
    private Set<KafkaBroker> replicas;
    private Set<KafkaBroker> isrs;
    private short errorCode = 0;

    public PartitionMetadata(int partitionId, KafkaBroker leader, Set<KafkaBroker> replicas,
                             Set<KafkaBroker> isrs, short errorCode) {
        this.partitionId = partitionId;
        this.replicas = replicas;
        this.isrs = isrs;
        this.leader = leader;
        this.errorCode = errorCode;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public KafkaBroker getLeader() {
        return leader;
    }

    public void setLeader(KafkaBroker leader) {
        this.leader = leader;
    }

    public Set<KafkaBroker> getReplicas() {
        return replicas;
    }

    public void setReplicas(Set<KafkaBroker> replicas) {
        this.replicas = replicas;
    }

    public Set<KafkaBroker> getIsr() {
        return isrs;
    }

    public void setIsr(Set<KafkaBroker> isrs) {
        this.isrs = isrs;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
    }

    @Override
    public String toString() {
        StringBuilder partitionMetadataString = new StringBuilder();
        partitionMetadataString.append("\tpartition " + partitionId);
        return partitionMetadataString.toString();
    }
}