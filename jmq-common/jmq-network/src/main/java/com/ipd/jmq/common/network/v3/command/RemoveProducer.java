package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.network.v3.session.ProducerId;
import com.ipd.jmq.toolkit.lang.Preconditions;

public class RemoveProducer extends JMQPayload {
    //生产者ID
    private ProducerId producerId;


    public RemoveProducer producerId(final ProducerId producerId) {
        this.producerId = producerId;
        return this;
    }

    public ProducerId getProducerId() {
        return this.producerId;
    }

    public void setProducerId(ProducerId producerId) {
        this.producerId = producerId;
    }


    public int predictionSize() {
        return Serializer.getPredictionSize(producerId.getProducerId()) + 1;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkArgument(producerId != null, "producer ID can not be null.");
    }

    @Override
    public int type() {
        return CmdTypes.REMOVE_PRODUCER;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RemoveProducer{");
        sb.append("producerId=").append(producerId);
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

        RemoveProducer that = (RemoveProducer) o;

        if (producerId != null ? !producerId.equals(that.producerId) : that.producerId != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (producerId != null ? producerId.hashCode() : 0);
        return result;
    }
}