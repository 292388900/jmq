package com.ipd.jmq.common.network.v3.command;

/**
 * 合并消费位置
 *
 * @author lindeqiang
 */
public class GetConsumeOffset extends JMQPayload {
    // offset文件数据内容
    protected String offset;
    protected boolean isSlaveConsume;

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }

    public boolean isSlaveConsume() {
        return isSlaveConsume;
    }

    public void setSlaveConsume(boolean slaveConsume) {
        isSlaveConsume = slaveConsume;
    }

    public int predictionSize() {
        int size = 4 + 1;
        if (offset != null && !offset.isEmpty()) {
            if (offset.length() < 100) {
                size += 300;
            } else {
                size += Serializer.getPredictionSize(offset, 0);
            }
        }
        return size;
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

        GetConsumeOffset that = (GetConsumeOffset) o;

        if (offset != null ? !offset.equals(that.offset) : that.offset != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (offset != null ? offset.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return offset;
    }

    @Override
    public int type() {
        return CmdTypes.GET_CONSUMER_OFFSET;
    }
}
