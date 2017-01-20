package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.network.v3.session.ConnectionId;
import com.ipd.jmq.toolkit.lang.Preconditions;

/**
 * 删除连接
 */
public class RemoveConnection extends JMQPayload {
    // 连接ID
    private ConnectionId connectionId;

    public RemoveConnection() {
    }

    public RemoveConnection connectionId(final ConnectionId connectionId) {
        setConnectionId(connectionId);
        return this;
    }

    public ConnectionId getConnectionId() {
        return this.connectionId;
    }

    public void setConnectionId(ConnectionId connectionId) {
        this.connectionId = connectionId;
    }

    @Override
    public int predictionSize() {
        return Serializer.getPredictionSize(connectionId.getConnectionId()) + 1;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkArgument(connectionId != null, "connectionId can not be null.");
    }

    @Override
    public int type() {
        return CmdTypes.REMOVE_CONNECTION;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RemoveConnection{");
        sb.append("connectionId=").append(connectionId);
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

        RemoveConnection that = (RemoveConnection) o;

        if (connectionId != null ? !connectionId.equals(that.connectionId) : that.connectionId != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (connectionId != null ? connectionId.hashCode() : 0);
        return result;
    }
}