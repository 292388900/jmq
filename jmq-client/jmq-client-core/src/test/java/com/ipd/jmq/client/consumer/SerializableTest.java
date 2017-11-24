package com.ipd.jmq.client.consumer;

import com.ipd.jmq.client.cluster.ClusterManager;
import com.ipd.jmq.client.consumer.offset.FileLocalOffsetStore;
import com.ipd.jmq.client.consumer.offset.LocalMessageQueue;
import com.ipd.jmq.common.cluster.BrokerCluster;
import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.message.BroadcastQueue;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class SerializableTest {
    // 集群
    protected ConcurrentMap<String, BrokerCluster> clusters = new ConcurrentHashMap<String, BrokerCluster>();
    // 主题配置
    protected ConcurrentMap<String, TopicConfig> topicConfigs = new ConcurrentHashMap<String, TopicConfig>();
    // 主题配置字符串

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testMapSerializable() throws Exception {
        short queue1 = 1;
        short queue2 = 2;
        Set<LocalMessageQueue> messageQueueSet = new HashSet<LocalMessageQueue>();
        AtomicLong atomicLong1 = new AtomicLong();
        AtomicLong atomicLong2 = new AtomicLong();
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopic("broadcast");
        FileLocalOffsetStore fileLocalOffsetStore = new FileLocalOffsetStore(System.getProperty("user.home"),topicConfig.getTopic(),"app");
        LocalMessageQueue messageQueue1 = new LocalMessageQueue("192.168.179.67_50088","test1", "app", queue1);
        atomicLong1.set(4);
        LocalMessageQueue messageQueue2 = new LocalMessageQueue("192.168.179.67_50088","test1", "app", queue1);
        atomicLong2.set(5);
        fileLocalOffsetStore.getOffsetTable().putIfAbsent(messageQueue1, atomicLong1);
        fileLocalOffsetStore.getOffsetTable().putIfAbsent(messageQueue2, atomicLong2);
        for(LocalMessageQueue messageQueue : fileLocalOffsetStore.getOffsetTable().keySet()){
            messageQueueSet.add(messageQueue);
        }
        fileLocalOffsetStore.persist();
        fileLocalOffsetStore.getOffsetTable().clear();
        if(!fileLocalOffsetStore.getOffsetTable().isEmpty()){
            System.out.println(fileLocalOffsetStore.getOffsetTable().entrySet().iterator().next().getValue());
        }
        try {
            fileLocalOffsetStore.start();
            for(LocalMessageQueue messageQueue : fileLocalOffsetStore.getOffsetTable().keySet()){
                System.out.println(messageQueue.getQueueId());
                System.out.println(messageQueue.getTopic());
                System.out.println(fileLocalOffsetStore.getOffsetTable().get(messageQueue));
            }
        }catch (Exception e){
            System.out.println(e.getStackTrace());
        }
    }
}
