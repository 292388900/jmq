package com.ipd.jmq.server.broker.dispatch;

import java.util.Map;

/**
 * 需要迁移的队列.
 *
 * @author lindeqiang
 * @since 2016/8/25 15:07
 */
public class MigrateQueues {
    // 即将迁移走的
    private Map<Short, String> from;
    // 即将迁移进的
    private Map<Short, String> to;

    public MigrateQueues(Map<Short, String> from, Map<Short, String> to) {
        this.from = from;
        this.to = to;
    }

    public Map<Short, String> getFrom() {
        return from;
    }

    public void setFrom(Map<Short, String> from) {
        this.from = from;
    }

    public Map<Short, String> getTo() {
        return to;
    }

    public void setTo(Map<Short, String> to) {
        this.to = to;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MigrateQueues that = (MigrateQueues) o;

        if (from != null ? !from.equals(that.from) : that.from != null) return false;
        return to != null ? to.equals(that.to) : that.to == null;

    }

    @Override
    public int hashCode() {
        int result = from != null ? from.hashCode() : 0;
        result = 31 * result + (to != null ? to.hashCode() : 0);
        return result;
    }


}
