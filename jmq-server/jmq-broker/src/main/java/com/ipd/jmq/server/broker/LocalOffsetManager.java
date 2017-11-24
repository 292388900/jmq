package com.ipd.jmq.server.broker;

import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
import com.ipd.jmq.server.store.Store;
import com.ipd.jmq.toolkit.concurrent.NamedThreadFactory;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhangkepeng on 15-8-26.
 */
public class LocalOffsetManager extends Service {
    private static final Logger logger = LoggerFactory.getLogger(LocalOffsetManager.class);
    // Broker配置
    private BrokerConfig config;
    // 集群管理
    private ClusterManager clusterManager;
    // 派发服务
    protected DispatchService dispatchService;
    // 广播消息确认线程
    protected ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory("JMQ_SERVER_EXPIRE_ACK_SCHEDULE"));
    public LocalOffsetManager(){}
    public LocalOffsetManager(BrokerConfig config, ClusterManager clusterManager, DispatchService dispatchService) {
        this.config = config;
        this.clusterManager = clusterManager;
        this.dispatchService = dispatchService;
    }

    public void setConfig(BrokerConfig config) {
        this.config = config;
    }

    public void setClusterManager(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setDispatchService(DispatchService dispatchService) {
        this.dispatchService = dispatchService;
    }

    @Override
    public void doStart() {
        scheduler.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    localOffsetAck();
                } catch (Exception e) {
                    logger.error("ScheduledTask persistAllConsumerOffset exception", e);
                }
            }
        }, 1000 * 300, config.getStoreConfig().getBroadcastOffsetAckInterval(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void doStop() {
        Close.close(scheduler);
        scheduler = null;
    }

    public void localOffsetAck() throws JMQException {
        consumerFromLocalOffsetAppAck();
    }

    /**
     * 本地消费者消费位置确认
     */
    private void consumerFromLocalOffsetAppAck() throws JMQException {
        Map<String, List<String>> localOffsetConsumers = findConsumerFromLocalOffsetApp(clusterManager);
        if (localOffsetConsumers == null || localOffsetConsumers.isEmpty()) {
            return;
        }
        Store jmqStore = config.getStore();
        if (!jmqStore.isReady()) {
            return;
        }
        Iterator<Map.Entry<String, List<String>>> entries = localOffsetConsumers.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, List<String>> entry = entries.next();
            for (String topic : entry.getValue()) {
                dispatchService.ackConsumerExpiredMessage(topic, entry.getKey());
            }
        }
    }

    /**
     * 查找所有本地消费的消费者
     *
     * @param clusterManager
     * @return
     */
    private Map<String, List<String>> findConsumerFromLocalOffsetApp(ClusterManager clusterManager) {
        Map<String, TopicConfig> allTopicInfo;
        TopicConfig topicConfig;
        Map<String, List<String>> localOffsetConsumers = new HashMap<String, List<String>>();
        Map<String, TopicConfig.ConsumerPolicy> consumers;
        allTopicInfo = clusterManager.getTopics();
        if (allTopicInfo != null && !allTopicInfo.isEmpty()) {
            for (String topic : allTopicInfo.keySet()) {
                topicConfig = allTopicInfo.get(topic);
                consumers = topicConfig.getConsumers();
                if (consumers != null && !consumers.isEmpty()) {
                    for (String consumer : consumers.keySet()) {
                        if (topicConfig.isLocalManageOffsetConsumer(consumer)) {
                            List<String> topics = localOffsetConsumers.get(consumer);
                            if (topics == null) {
                                topics = new ArrayList<String>();
                                localOffsetConsumers.put(consumer, topics);
                            }
                            topics.add(topic);
                        }
                    }
                } else {
                    continue;
                }
            }
        }
        return localOffsetConsumers;
    }
}
