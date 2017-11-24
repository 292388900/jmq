package com.ipd.jmq.common.network.kafka.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhangkepeng on 16-8-10.
 *
 * zookeeper节点数据序列化类
 */
public class TopicsBroker implements Serializable {

    private int version = 1;
    private Map<String, Set<Integer>> partitions = new HashMap<String, Set<Integer>>();

    public TopicsBroker() {

    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public Map<String, Set<Integer>> getPartitions() {
        return partitions;
    }

    public void setPartitions(Map<String, Set<Integer>> partitions) {
        this.partitions = partitions;
    }
}
