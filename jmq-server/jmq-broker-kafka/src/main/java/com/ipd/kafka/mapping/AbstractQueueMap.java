package com.ipd.kafka.mapping;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhangkepeng on 16-12-16.
 */
public abstract class AbstractQueueMap<T> {

    // 本地分组Partition与Queue映射缓存
    public Map<T, Short> PARTITION2QUEUE = new HashMap<T, Short>();

    // 本地分组Queue与Partition映射缓存
    public Map<Short, T> QUEUE2PARTITION = new HashMap<Short, T>();

    abstract void queueMap(Set<T> partitions);
}
