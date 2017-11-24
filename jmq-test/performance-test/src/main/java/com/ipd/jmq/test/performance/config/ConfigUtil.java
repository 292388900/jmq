package com.ipd.jmq.test.performance.config;

import com.alibaba.fastjson.JSON;
import com.ipd.jmq.common.cluster.*;

import com.ipd.jmq.registry.RegistryException;
import com.ipd.jmq.registry.zookeeper.ZKRegistry;
import com.ipd.jmq.toolkit.io.Compressors;
import com.ipd.jmq.toolkit.io.Zip;
import com.ipd.jmq.toolkit.lang.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by zhangkepeng on 2017/1/9.
 */
public class ConfigUtil {
    private final static Logger logger = LoggerFactory.getLogger(ConfigUtil.class);

    private static String BROKER_UPDATE = "/jmq/broker_zip";
    private static String TOPIC_UPDATA = "/jmq/topic_zip";
    private static String QUEUE_UPDATA = "/jmq/queues_zip";

    public static ZKRegistry registry;

    private static int id = 0;

    public static void brokerUpdate(String brokerStr) {
        if (brokerStr == null) {
            return;
        }
        String[] brokers = brokerStr.split(";");
        List<Broker> brokerList = toBroker(brokers);
        if (brokerList == null || brokerList.isEmpty()) {
            logger.error("no broker written to zk");
            return;
        }
        String json = JSON.toJSONString(brokerList);
        byte[] bytes = json.getBytes(Charsets.UTF_8);
//        byte[] bytes = null;
//        try {
//            bytes = Compressors.compress(json, Charsets.UTF_8, Zip.INSTANCE);
//        } catch (IOException e) {
//            logger.error(e.getMessage(), e);
//        }
        if (registry != null) {
            try {
                registry.update(BROKER_UPDATE, bytes);
            } catch (RegistryException e) {
                logger.error("update broker exception");
            }
        }
    }

    public static void partitionUpdate(String topicsInfo) {
        if (topicsInfo == null) {
            return;
        }
        String[] topicConfigArray = topicsInfo.split(";");
        if (topicConfigArray.length == 0) {
            return;
        }
        List<TopicConfig> topicConfigs = new ArrayList<TopicConfig>();
        for (String topicConfigStr : topicConfigArray) {
            TopicConfig topicConfig = toTopicConfig(topicConfigStr);
            if (topicConfig != null) {
                topicConfigs.add(topicConfig);
            }
        }
        if (!topicConfigs.isEmpty()) {
            String json = JSON.toJSONString(topicConfigs);
            byte[] bytes = json.getBytes(Charsets.UTF_8);
//            byte[] bytes = null;
//            try {
//                bytes = Compressors.compress(json, Charsets.UTF_8, Zip.INSTANCE);
//            } catch (IOException e) {
//                logger.error(e.getMessage(), e);
//            }
            try {
                registry.update(TOPIC_UPDATA, bytes);
            } catch (RegistryException e) {
                logger.error("update topic exception");
                return;
            }
        }
        updateTopicQueues(topicConfigs);
    }
    // 更新kafka分片信息
    private static void updateTopicQueues(List<TopicConfig> topicConfigs) {
        if (topicConfigs == null || topicConfigs.isEmpty()) {
            return;
        }
        List<TopicQueues> topicQueuesList = new ArrayList<TopicQueues>();
        for (TopicConfig topicConfig : topicConfigs) {
            Map<String, List<Short>> topicQues = new HashMap<String, List<Short>>();
            short partition = 0;
            Set<String> groupSet = topicConfig.getGroups();
            for (String group : groupSet) {
                List<Short> queueList = new ArrayList<Short>();
                short queues = topicConfig.getQueues();
                for (short queue = 0; queue < queues; queue++) {
                    queueList.add(partition);
                    partition++;
                }
                topicQues.put(group, queueList);
            }
            TopicQueues topicQueues = new TopicQueues();
            topicQueues.setTopic(topicConfig.getTopic());
            topicQueues.setGroups(topicQues);
            topicQueuesList.add(topicQueues);
        }
        String json = JSON.toJSONString(topicQueuesList);
        byte[] bytes = json.getBytes(Charsets.UTF_8);
//        byte[] bytes = null;
//        try {
//            bytes = Compressors.compress(json, Charsets.UTF_8, Zip.INSTANCE);
//        } catch (IOException e) {
//            logger.error(e.getMessage(), e);
//        }
        try {
            registry.update(QUEUE_UPDATA, bytes);
        } catch (RegistryException e) {
            logger.error("update topic exception");
            return;
        }
    }

    private static TopicConfig toTopicConfig(String topicConfigStr) {
        TopicConfig topicConfig = null;
        String[] topicConfigInfo = topicConfigStr.split(":");
        if (topicConfigInfo.length != 4) {
            return null;
        }
        String topic = topicConfigInfo[0];
        String producers = topicConfigInfo[1];
        String consumers = topicConfigInfo[2];
        String groups = topicConfigInfo[3];
        topicConfig = new TopicConfig();
        Map<String, TopicConfig.ConsumerPolicy> consumerPolicyMap = new HashMap<String, TopicConfig.ConsumerPolicy>();
        String[] consumerArray = consumers.split(",");
        for (String consumer : consumerArray) {
            consumerPolicyMap.put(consumer, new TopicConfig.ConsumerPolicy());
        }
        topicConfig.setConsumers(consumerPolicyMap);
        Map<String, TopicConfig.ProducerPolicy> producerPolicyMap = new HashMap<String, TopicConfig.ProducerPolicy>();
        String[] producerArray = producers.split(",");
        for (String producer : producerArray) {
            producerPolicyMap.put(producer, new TopicConfig.ProducerPolicy());
        }
        Set<String> groupSet = new HashSet<String>();
        String[] groupArray = groups.split(",");
        Collections.addAll(groupSet, groupArray);
        /*for (String group : groupArray) {
            groupSet.add(group);
        }*/
        topicConfig.setArchive(false);
        topicConfig.setConsumers(consumerPolicyMap);
        topicConfig.setGroups(groupSet);
        topicConfig.setProducers(producerPolicyMap);
        topicConfig.setQueues((short) 6);
        topicConfig.setTopic(topic);
        topicConfig.setType(TopicConfig.TopicType.TOPIC);
        return topicConfig;
    }

    private static List<Broker> toBroker(String[] brokers) {
        List<Broker> brokerList = new ArrayList<Broker>();
        for (String broker : brokers) {
            String[] groupBroker = broker.split("@");
            if (groupBroker.length != 2) {
                return null;
            }
            String group = groupBroker[0];
            String brokerStr = groupBroker[1];
            String[] brokerInfos = brokerStr.split(",");

            if (brokerInfos.length >= 1) {
                if (brokerInfos.length == 1) {
                    String masterStr = brokerInfos[0];
                    Broker master = setBroker(masterStr);
                    if (master != null) {
                        master.setGroup(group);
                        brokerList.add(master);
                    }
                } else {
                    String masterStr = brokerInfos[0];
                    Broker master = setBroker(masterStr);
                    if (master != null) {
                        master.setGroup(group);
                        brokerList.add(master);
                    }

                    String slaveStr = brokerInfos[1];

                    Broker slave = setBroker(slaveStr);
                    if (slave != null) {
                        slave.setGroup(group);
                        brokerList.add(slave);
                    }
                }
            }
        }
        return brokerList;
    }

    private static Broker setBroker(String brokerInfo) {
        Broker broker = null;
        if (brokerInfo == null) {
            return null;
        }
        String[] ipPort = brokerInfo.split(":");
        if (ipPort.length != 2) {
            return null;
        }
        String ip = ipPort[0];
        int port = Integer.valueOf(ipPort[1]);
        broker = new Broker(ip, port);
        broker.setId(id++);
        broker.setDataCenter((byte)0);
        broker.setName(broker.getName());
        broker.setIp(ip);
        broker.setPort(port);
        broker.setSyncMode(SyncMode.SYNCHRONOUS);
        broker.setRetryType(RetryType.REMOTE);
        if (port % 2 == 0) {
            broker.setPermission(Permission.FULL);
            broker.setRole(ClusterRole.MASTER);
        } else {
            broker.setPermission(Permission.NONE);
            broker.setRole(ClusterRole.SLAVE);
        }
        return broker;
    }
}
