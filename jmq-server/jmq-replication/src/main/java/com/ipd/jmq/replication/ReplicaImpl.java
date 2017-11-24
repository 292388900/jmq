package com.ipd.jmq.replication;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.network.Transport;

import java.net.SocketAddress;

/**
 * Created by guoliang5 on 2016/8/23.
 */
public class ReplicaImpl implements Replica, Comparable<ReplicaImpl> {
    public ReplicaImpl(Broker broker, Transport transport, State state) {
        broker_ = broker;
        transport_ =transport;
        state_ = state;
    }

    @Override
    public State getState() {
        return state_;
    }

    @Override
    public long getReplicatedOffset() {
        return replicatedOffset_;
    }

    @Override
    public Broker getBroker() {
        return broker_;
    }

    @Override
    public Transport getTransport() {
        return transport_;
    }

    @Override
    public SocketAddress getAddress() {
        return transport_.remoteAddress();
    }

    @Override
    public void close() {
        if (transport_ != null) {
            transport_.stop();
            transport_ = null;
        }
    }

    public void setState(State state) {
        this.state_ = state;
    }

    public void setTransport(Transport transport) {
        this.transport_ = transport;
    }

    public void setReplicatedOffset(long replicatedOffset) {
        this.replicatedOffset_ = replicatedOffset;
    }

    public void setBroker(Broker broker) {
        this.broker_ = broker;
    }

    @Override
    public int compareTo(ReplicaImpl o) {
        return Long.compare(replicatedOffset_, o.getReplicatedOffset());
    }

    private State state_;
    private Transport transport_;
    private long replicatedOffset_ = 0;
    private Broker broker_;
}
