package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.toolkit.lang.Preconditions;

/**
 * 复制身份信息
 */
public class Identity extends JMQPayload {
    // Broker
    private Broker broker;


    public Identity broker(final Broker broker) {
        setBroker(broker);
        return this;
    }

    public Broker getBroker() {
        return this.broker;
    }


    public void setBroker(Broker broker) {
        this.broker = broker;
    }

    public int predictionSize() {
        return Serializer.getPredictionSize(broker.getAlias()) + 7 + 1;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkArgument(broker!=null, "broker can not be null");
    }

    @Override
    public int type() {
        return CmdTypes.IDENTITY;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Identity{");
        sb.append("broker=").append(broker);
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

        Identity identity = (Identity) o;

        if (broker != null ? !broker.equals(identity.broker) : identity.broker != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (broker != null ? broker.hashCode() : 0);
        return result;
    }

}