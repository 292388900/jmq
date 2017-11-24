package com.ipd.jmq.common.cluster;

/**
 * 迁移状态.
 *
 * @author lindeqiang
 * @since 2016/9/1 13:43
 */
public class MigrateState {
    private String topic;
    private short queueId;
    private State state = State.UNKNOWN;

    public MigrateState() {
    }

    public MigrateState(short queueId, State state) {
        this.queueId = queueId;
        this.state = state;
    }

    public short getQueueId() {
        return queueId;
    }

    public void setQueueId(short queueId) {
        this.queueId = queueId;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public enum State {
        NEW, RUNNING, FINISHED, UNKNOWN ;

        public static State valueOf(final int value) {
            switch (value) {
                case 0:
                    return NEW;
                case 1:
                    return RUNNING;
                case 2:
                    return FINISHED;
                default:
                    return UNKNOWN;
            }
        }
    }
}
