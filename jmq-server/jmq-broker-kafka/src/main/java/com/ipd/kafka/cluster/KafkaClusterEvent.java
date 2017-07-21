package com.ipd.kafka.cluster;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangkepeng on 16-8-24.
 */
public abstract class KafkaClusterEvent {
    // 类型
    protected EventType type;

    public EventType getType() {
        return this.type;
    }

    public void setType(EventType type) {
        this.type = type;
    }

    public enum EventType {
        /**
         * partition级别删除
         */
        PARTITION_REMOVED(1),
        /**
         * partition级别修改
         */
        PARTITION_UPDATED(2),
        /**
         * topic级别修改
         */
        TOPIC_PARTITIONS_UPDATED(3);

        int type;

        EventType(int type) {
            this.type = type;
        }

        public int getType() {
            return type;
        }

        private static Map<Integer, EventType> types = new HashMap<Integer, EventType>();

        /**
         * 获取类别
         *
         * @param type
         * @return
         */
        public static EventType valueOf(int type) {
            if (types.isEmpty()) {
                synchronized (types) {
                    if (types.isEmpty()) {
                        for (EventType topicsBrokerType : EventType.values()) {
                            types.put(topicsBrokerType.type, topicsBrokerType);
                        }
                    }
                }
            }
            return types.get(type);
        }
    }
}
