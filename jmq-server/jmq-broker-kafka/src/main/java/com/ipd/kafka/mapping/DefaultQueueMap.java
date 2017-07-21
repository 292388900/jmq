package com.ipd.kafka.mapping;

import com.ipd.jmq.common.message.MessageQueue;
import java.util.Set;

/**
 * Created by zhangkepeng on 16-12-16.
 */
public class DefaultQueueMap extends AbstractQueueMap<Integer> {

    @Override
    public void queueMap(Set<Integer> partitions) {
        // partitions 从小到大排好序
        boolean firstPartition = true;
        short queueCount = 1;
        for (int partition : partitions) {
            if (firstPartition) {
                PARTITION2QUEUE.put(partition, MessageQueue.HIGH_PRIORITY_QUEUE);
                QUEUE2PARTITION.put(MessageQueue.HIGH_PRIORITY_QUEUE, partition);
                firstPartition = false;
            } else {
                PARTITION2QUEUE.put(partition, queueCount);
                QUEUE2PARTITION.put(queueCount, partition);
                queueCount++;
            }
        }
    }
}
