package broker.cluster;

import com.alibaba.fastjson.JSON;
import com.ipd.jmq.common.cluster.TopicQueues;
import com.ipd.jmq.registry.Registry;
import com.ipd.jmq.registry.RegistryFactory;
import com.ipd.jmq.toolkit.io.Compressors;
import com.ipd.jmq.toolkit.io.Zip;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lindeqiang
 * @since 2016/9/8 11:22
 */
public class GroupQueueConfig {
    private static final Logger logger = LoggerFactory.getLogger(GroupQueueConfig.class);
    // 队列分布路径
    private static final String queuePath = "/jmq/queue_zip";
    protected RegistryFactory registryFactory;
    protected Registry registry;

    @Before
    public void setUp() throws Exception {
        registryFactory = new RegistryFactory("zookeeper://192.168.179.67:2181");
        registry = registryFactory.create();
        registry.start();
    }

    @Test
    public void testUpdateTopicQueues() throws Exception {
        Map<String, List<Short>> formalQueues = new HashMap<String, List<Short>>();
        String group = "octopus1";
        String topic = "octopus_test";
        int queueCount = 7;

        List<Short> queues = new ArrayList<>(queueCount);
        for (short i = 1; i < queueCount; i++) {
            queues.add(i);
        }

        formalQueues.put(group, queues);
        TopicQueues topicQueues = new TopicQueues();
        topicQueues.setTopic(topic);
        topicQueues.setGroups(formalQueues);

        List<TopicQueues> topicQueuesList = new ArrayList<>();
        topicQueuesList.add(topicQueues);

        String content = JSON.toJSONString(topicQueuesList);

        byte[] data = Compressors.compress(content, Zip.INSTANCE);
        registry.update(queuePath, data);

        logger.info("update queue config success! config [{}]", content);
    }
}