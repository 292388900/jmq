package broker.kafka.registry;

import com.alibaba.fastjson.JSON;
import com.ipd.jmq.common.cluster.*;
import com.ipd.jmq.common.network.kafka.model.TopicState;
import com.ipd.jmq.common.network.kafka.utils.ZKUtils;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.registry.RegistryException;
import com.ipd.jmq.registry.listener.ChildrenDataListener;
import com.ipd.jmq.registry.listener.ChildrenEvent;
import com.ipd.jmq.registry.zookeeper.ZKRegistry;
import com.ipd.jmq.toolkit.URL;
import com.ipd.jmq.toolkit.lang.Charsets;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

/**
 * Created by zhangkepeng on 16-8-10.
 */
public class ZKUtilsTest {

    private static ZKRegistry registry = new ZKRegistry(URL.valueOf("zookeeper://localhost"));
    private static String brokerPath;
    private static String topicPatitionPath;
    private static BrokerConfig config = new BrokerConfig();

    @BeforeClass
    public static void setup() throws Exception {
        registry.start();
        brokerPath = config.getBrokerPath();
        topicPatitionPath = ZKUtils.getTopicPartitionPath();
    }

    @AfterClass
    public static void destroy() throws RegistryException {
        registry.stop();
    }

    @Test
    public void testPersistentPath() {
        Set<Integer> isrs = new HashSet<Integer>();
        isrs.add(0);
        TopicState topicState = new TopicState(0, isrs);
        String json = JSON.toJSONString(topicState);
        Assert.assertTrue(ZKUtils.createPersistentPath(registry, ZKUtils.getTopicPath("test"), "{\"version\":1,\"partitions\":{\"0\":[0,0]}}"));
        Assert.assertTrue(ZKUtils.createPersistentPath(registry, ZKUtils.getTopicPartitionLeaderAndIsrPath("test", 0), json));
    }

    @Test
    public void testAddListener() {
        ChildrenDataListener childrenDataListener = new ChildrenDataListener() {
            @Override
            public void onEvent(ChildrenEvent childrenEvent) {
                System.out.println("children path is " + childrenEvent.getPath());
                System.out.println("children data is " + new String(childrenEvent.getData(), Charsets.UTF_8));
            }
        };
        ZKUtils.createPersistentPath(registry, ZKUtils.getTopicPath("test"), "");
        registry.addListener(ZKUtils.getTopicPartitionsPath("test"), childrenDataListener);

        ZKUtils.createPersistentPath(registry, ZKUtils.getTopicPartitionPath("test", 1), "partition one");
        ZKUtils.updatePersistentPath(registry, ZKUtils.getTopicPartitionPath("test", 1), "partition two");
    }

    @Test
    public void testBrokerConfig() {
        List<Broker> brokers = new ArrayList<Broker>();
        Broker broker = new Broker();
        broker.setId(1);
        broker.setDataCenter((byte)1);
        broker.setName("10_12_165_66_50088");
        broker.setIp("10.12.165.66");
        broker.setPort(50088);
        broker.setGroup("kafka1");
        broker.setAlias("kafka1_m");
        broker.setPermission(Permission.FULL);
        broker.setRole(ClusterRole.MASTER);
        broker.setSyncMode(SyncMode.SYNCHRONOUS);
        broker.setRetryType(RetryType.DB);
        brokers.add(broker);
        String json = JSON.toJSONString(brokers);
        Assert.assertTrue("update broker_config pass", ZKUtils.updatePersistentPath(registry, "/jmq/broker_zip", json));
    }

    @Test
    public void testTopicConfig() {
        List<TopicConfig> topicConfigs = new ArrayList<TopicConfig>();
        String topic = "test";
//        String consumer = "Client_" + topic + "_" + partition;
//        String producer = "Client_" + topic + "_" + partition;
        String consumer = "test";
        String producer = "test";
        String group = "kafka1";
        Set<String> groups = new HashSet<String>();
        groups.add(group);
        TopicConfig topicConfig = new TopicConfig();

        topicConfig.setArchive(false);
        Map<String, TopicConfig.ConsumerPolicy> consumers = new HashMap<String, TopicConfig.ConsumerPolicy>();
        consumers.put(consumer, new TopicConfig.ConsumerPolicy());
        topicConfig.setConsumers(consumers);
        topicConfig.setGroups(groups);
        topicConfig.setImportance(2);
        Map<String, TopicConfig.ProducerPolicy> producers = new HashMap<String, TopicConfig.ProducerPolicy>();
        producers.put(producer, new TopicConfig.ProducerPolicy());
        topicConfig.setProducers(producers);
        topicConfig.setQueues((short) 5);
        topicConfig.setTopic(topic);
        topicConfig.setType(TopicConfig.TopicType.TOPIC);
        topicConfigs.add(topicConfig);
        String json = JSON.toJSONString(topicConfigs);
        Assert.assertTrue("update topic_config pass", ZKUtils.updatePersistentPath(registry, "/jmq/topic_zip", json));
    }

    @Test
    public void testTopicQueues() {
        List<TopicQueues> topicQueuesList = new ArrayList<TopicQueues>();
        String topic = "test";
        String group = "kafka1";
        TopicQueues topicQueues = new TopicQueues();
        topicQueues.setTopic(topic);
        Map<String, List<Short>> topicQues = new HashMap<String, List<Short>>();
        List<Short> queues = new ArrayList<Short>();
        queues.add((short)0);
        queues.add((short)1);
        topicQues.put(group, queues);
        topicQueues.setGroups(topicQues);
        topicQueuesList.add(topicQueues);
        String json = JSON.toJSONString(topicQueuesList);
        Assert.assertTrue("update queues pass", ZKUtils.updatePersistentPath(registry, "/jmq/queues_zip", json));
    }

}
