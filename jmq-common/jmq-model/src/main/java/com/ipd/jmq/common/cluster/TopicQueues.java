package com.ipd.jmq.common.cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lindeqiang
 * @since 2016/8/22 20:12
 */
public class TopicQueues {
    // 主题
    private String topic;
    // 分组与队列Map
    private Map<String, List<Short>> groups = new HashMap<String, List<Short>>();

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Map<String, List<Short>> getGroups() {
        return groups;
    }

    public void setGroups(Map<String, List<Short>> groups) {
        this.groups = groups;
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == null || !(obj instanceof TopicQueues)) {
            return false;
        }

        TopicQueues that = (TopicQueues) obj;

        if (that.getTopic() != null && !that.getTopic().equals(topic)) {
            return false;
        }

        if (topic != null && !topic.equals(that.getTopic())) {
            return false;
        }

        if (!groups.equals(that.getGroups())) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = result + 31 * (topic == null ? 0 : topic.hashCode());
        result = result + 31 * (groups == null ? 0 : groups.hashCode());
        return result;
    }
}
