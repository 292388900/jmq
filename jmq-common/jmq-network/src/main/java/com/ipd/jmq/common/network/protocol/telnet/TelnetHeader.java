package com.ipd.jmq.common.network.protocol.telnet;


import com.ipd.jmq.common.model.Acknowledge;
import com.ipd.jmq.common.network.command.Direction;
import com.ipd.jmq.common.network.command.Header;

/**
 * Created by hexiaofeng on 16-7-7.
 */
public class TelnetHeader implements Header {

    protected int requestId;
    protected Direction direction;
    protected Acknowledge acknowledge;

    @Override
    public int getRequestId() {
        return requestId;
    }

    @Override
    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }

    @Override
    public Direction getDirection() {
        return direction;
    }

    @Override
    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    @Override
    public Acknowledge getAcknowledge() {
        return acknowledge;
    }

    @Override
    public void setAcknowledge(Acknowledge acknowledge) {
        this.acknowledge = acknowledge;
    }

    /**
     * 构造器
     */
    public static class Builder {
        TelnetHeader header = new TelnetHeader();

        public static Builder create() {
            return new Builder();
        }

        public static TelnetHeader response() {
            return create().direction(Direction.RESPONSE).acknowledge(Acknowledge.ACK_NO).build();
        }

        public static TelnetHeader request() {
            return create().direction(Direction.REQUEST).acknowledge(Acknowledge.ACK_RECEIVE).build();
        }

        public Builder requestId(int requestId) {
            header.setRequestId(requestId);
            return this;
        }

        public Builder direction(Direction direction) {
            header.setDirection(direction);
            return this;
        }

        public Builder acknowledge(Acknowledge acknowledge) {
            header.setAcknowledge(acknowledge);
            return this;
        }

        public TelnetHeader build() {
            return header;
        }

    }
}
