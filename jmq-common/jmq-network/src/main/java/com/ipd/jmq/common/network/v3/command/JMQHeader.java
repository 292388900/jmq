package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.model.Acknowledge;
import com.ipd.jmq.toolkit.time.SystemClock;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * JMQ请求头.
 *
 * @author lindeqiang
 * @since 2016/7/20 10:33
 */
public class JMQHeader implements Header {
    public static final int HEADER_SIZE = 1 + 1 + 4 + 1 + 8;
    private static final AtomicInteger REQUEST_ID = new AtomicInteger(0);
    public static final byte VERSION = 3;
    // 版本号
    private byte version = VERSION;
    //请求ID
    protected int requestId;
    //请求or应答
    protected Direction direction;
    //命令类型
    protected int type;
    //请求或响应时间
    private long time;
    //响应状态码
    private int status;
    //响应错误信息
    private String error;
    //应答方式
    protected Acknowledge acknowledge = Acknowledge.ACK_FLUSH;

    public JMQHeader() {
    }

    public JMQHeader(Direction direction, int type) {
        this.direction = direction;
        this.type = type;
        this.time = SystemClock.now();
        if (this.direction == Direction.REQUEST) {
            this.requestId = REQUEST_ID.incrementAndGet();
        }
    }

    public JMQHeader(Direction direction, int type, long time) {
        this.direction = direction;
        this.type = type;
        this.time = time;
        if (this.direction == Direction.REQUEST) {
            this.requestId = REQUEST_ID.incrementAndGet();
        }
    }

    public JMQHeader(Direction direction, int type, int requestId, long time) {
        this.direction = direction;
        this.type = type;
        this.requestId = requestId;
        this.time = time;
        if (this.direction == Direction.REQUEST) {
            this.requestId = REQUEST_ID.incrementAndGet();
        }
    }

    public JMQHeader(Direction direction, int type, int requestId, int status, String error) {
        this.direction = direction;
        this.type = type;
        this.requestId = requestId;
        this.time = SystemClock.now();
        this.status = status;
        this.error = error;
        if (this.direction == Direction.REQUEST) {
            this.requestId = REQUEST_ID.incrementAndGet();
        }
    }

    public JMQHeader(Direction direction, int type, int requestId, long time, int status, String error) {
        this.direction = direction;
        this.type = type;
        this.requestId = requestId;
        this.time = time;
        this.status = status;
        this.error = error;
        if (this.direction == Direction.REQUEST) {
            this.requestId = REQUEST_ID.incrementAndGet();
        }
    }

    public byte getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public int getRequestId() {
        return requestId;
    }


    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }

    public Direction getDirection() {
        return direction;
    }

    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    public Acknowledge getAcknowledge() {
        return acknowledge;
    }

    public void setAcknowledge(Acknowledge acknowledge) {
        this.acknowledge = acknowledge;
    }

    public int getType() {
        return type;
    }

    public void setType(short type) {
        this.type = type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public int getPredictionSize() {
        if (direction == Direction.REQUEST) {
            return HEADER_SIZE;
        }
        return HEADER_SIZE + 1 + Serializer.getPredictionSize(error, 2);
    }

    /**
     * 构造器
     */
    public static class Builder {
        JMQHeader header = new JMQHeader();

        public static Builder create() {
            return new Builder();
        }

        public static JMQHeader response() {
            return create().direction(Direction.RESPONSE).acknowledge(Acknowledge.ACK_NO).build();
        }

        public static JMQHeader request() {
            return create().direction(Direction.REQUEST).acknowledge(Acknowledge.ACK_RECEIVE).build();
        }

        public static JMQHeader request(int type, Acknowledge acknowledge) {
            return create().direction(Direction.REQUEST).type(type).acknowledge(acknowledge).build();
        }

        public Builder requestId(int requestId) {
            header.setRequestId(requestId);
            return this;
        }

        public Builder direction(Direction direction) {
            header.setDirection(direction);
            if (header.direction == Direction.REQUEST) {
                header.requestId = REQUEST_ID.incrementAndGet();
            }
            return this;
        }

        public Builder acknowledge(Acknowledge acknowledge) {
            header.setAcknowledge(acknowledge);
            return this;
        }

        public Builder error(String error) {
            header.setError(error);
            return this;
        }

        public Builder status(int status) {
            header.setStatus(status);
            return this;
        }

        public Builder version(byte version){
            header.setVersion(version);
            return this;
        }

        public JMQHeader build() {
            return header;
        }

        public Builder type(int type) {
            header.setType(type);
            return this;
        }
    }
}
