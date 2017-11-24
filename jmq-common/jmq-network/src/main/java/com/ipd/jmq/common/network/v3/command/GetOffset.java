package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.toolkit.lang.Preconditions;

/**
 * 获取复制偏移量
 */
public class GetOffset extends JMQPayload {
    // 起始偏移量
    private long offset;
    // 优化
    private boolean optimized;

    public GetOffset optimized(final boolean optimized) {
        setOptimized(optimized);
        return this;
    }

    public GetOffset offset(final long offset) {
        setOffset(offset);
        return this;
    }

    public long getOffset() {
        return this.offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public boolean isOptimized() {
        return this.optimized;
    }

    public void setOptimized(boolean optimized) {
        this.optimized = optimized;
    }

    public int predictionSize() {
        return 9 + 1;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkArgument(offset > 0, "offset must be greater than or equal 0");
    }

    @Override
    public int type() {
        return CmdTypes.GET_OFFSET;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GetOffset{");
        sb.append("Offset=").append(offset);
        sb.append(", optimized=").append(optimized);
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

        GetOffset getOffset = (GetOffset) o;

        if (offset != getOffset.offset) {
            return false;
        }
        if (optimized != getOffset.optimized) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        result = 31 * result + (optimized ? 1 : 0);
        return result;
    }
}