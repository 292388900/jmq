package com.ipd.kafka.handler;

import com.ipd.jmq.common.network.kafka.model.DelayedResponseKey;
import com.ipd.jmq.common.network.kafka.model.KafkaBroker;
import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaHeader;
import com.ipd.jmq.common.network.kafka.command.TopicMetadataRequest;
import com.ipd.jmq.common.network.kafka.command.TopicMetadataResponse;
import com.ipd.jmq.common.network.kafka.exception.ErrorCode;
import com.ipd.jmq.common.network.kafka.model.TopicMetadata;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.handler.AbstractHandler;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.kafka.DelayedResponseManager;
import com.ipd.kafka.cluster.KafkaClusterManager;
import com.ipd.kafka.cluster.MetadataUpdater;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.toolkit.concurrent.Scheduler;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhangkepeng on 16-7-29.
 * Modified by luoruiheng
 */
public class TopicMetadataHandler extends AbstractHandler implements KafkaHandler {
    private static final Logger logger = LoggerFactory.getLogger(TopicMetadataHandler.class);

    //时间维度调用次数记数
    protected ConcurrentMap<String, ConcurrentMap<Long, AtomicInteger>> metricStat = new ConcurrentHashMap<String, ConcurrentMap<Long, AtomicInteger>>();
    protected long cacheWindowSize = 0;
    // 分片配置
    protected BrokerConfig brokerConfig;

    private DelayedResponseManager delayedResponseManager;
    private KafkaClusterManager kafkaClusterManager;

    public TopicMetadataHandler(DelayedResponseManager delayedResponseManager, KafkaClusterManager kafkaClusterManager, BrokerConfig brokerConfig, Scheduler cleanKafkaScheduler) {
        Preconditions.checkArgument(delayedResponseManager != null, "DelayedResponseManager can't be null");
        Preconditions.checkArgument(kafkaClusterManager != null, "KafkaClusterManager can't be null");
        Preconditions.checkArgument(brokerConfig != null, "BrokerConfig can't be null");
        Preconditions.checkArgument(cleanKafkaScheduler != null, "Scheduler can't be null");
        this.delayedResponseManager = delayedResponseManager;
        this.kafkaClusterManager = kafkaClusterManager;
        this.brokerConfig = brokerConfig;
        this.cacheWindowSize = brokerConfig.getStatTimeWindowSize();
        cleanKafkaScheduler.scheduleWithFixedDelay(new CleanRunnable(), 10000, 5 * 60 * 1000);
    }

    @Override
    public Command process(Transport transport, Command command) throws TransportException {
        if (command == null) {
            return null;
        }
        Command request = command;
        TopicMetadataRequest metadataRequest = (TopicMetadataRequest) request.getPayload();
        List<TopicMetadata> topicMetadatas = getTopicMetadata(metadataRequest.getTopics());
        Set<KafkaBroker> brokers = MetadataUpdater.metadataCache.getKafkaBrokers();
        KafkaHeader kafkaHeader = KafkaHeader.Builder.response(request.getHeader().getRequestId());
        Command response = new Command(kafkaHeader, new TopicMetadataResponse(brokers, topicMetadatas, metadataRequest.getCorrelationId()));
        /*DelayedResponseKey delayedResponseKey = new DelayedResponseKey(transport, command, response, DelayedResponseKey.Type.METADATA, 0L);
        if (!delayedResponseManager.suspend(delayedResponseKey)) {
            delayedResponseManager.closeTransportAndClearQueue(transport);
        }*/
        return response;
    }

    @Override
    public int[] type() {
        return new int[]{KafkaCommandKeys.METADATA};
    }

    private List<TopicMetadata> getTopicMetadata(Set<String> topics) {
        List<TopicMetadata> topicMetadatas = MetadataUpdater.metadataCache.getTopicMetadata(topics);

        // TODO: 3/14/17 未获取到元数据的，客户端会疯狂地不断拉取，暂时利用已有的限速策略，看考虑是否直接返回ERROR
        if (topicMetadatas.isEmpty()) {
            for (String topic : topics) {
                boolean isNeedLimit = isNeedLimit(topic, SystemClock.now(), cacheWindowSize, brokerConfig.getLimitTimes());
                if (isNeedLimit) {
                    // 频繁拉取元数据需要限速
                    logger.info(String.format("Topic:%s fetch metadata rate is limited", topic));
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        logger.info(String.format("Interrupted Exception when topic:%s fetch metadata rate is limited", topic));
                    }
                }
            }
        }

        Set<String> existTopics = kafkaClusterManager.getTopics().keySet();

        for (TopicMetadata topicMetadata : topicMetadatas) {
            String topic = topicMetadata.getTopic();
            if (null == topic || topic.isEmpty() || (!existTopics.isEmpty() && !existTopics.contains(topic))) {
                topicMetadata.setErrorCode(ErrorCode.INVALID_TOPIC);
                continue;
            }

            boolean isNeedLimit = isNeedLimit(topic, SystemClock.now(), cacheWindowSize, brokerConfig.getLimitTimes());
            if (isNeedLimit) {
                // 频繁拉取元数据需要限速
                logger.info(String.format("Topic:%s fetch metadata rate is limited", topic));
                try {
                    topicMetadata.setErrorCode(ErrorCode.NO_ERROR);
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.info(String.format("Interrupted Exception when topic:%s fetch metadata rate is limited", topic));
                }
            }
        }
        return topicMetadatas;
    }

    public class CleanRunnable implements Runnable {
        public void run() {
            if (logger.isDebugEnabled()) {
                logger.debug("start clean kafka metricStat");
            }
            try {
                //维度改变全部清理
                if (cacheWindowSize != brokerConfig.getStatTimeWindowSize()) {
                    metricStat.clear();
                    cacheWindowSize = brokerConfig.getStatTimeWindowSize();
                    return;
                }

                long curStatKey = getStatKey(SystemClock.now(), cacheWindowSize);
                List<String> emptyKey = new ArrayList<String>();
                for (Map.Entry<String, ConcurrentMap<Long, AtomicInteger>> entry : metricStat.entrySet()) {
                    Iterator<Map.Entry<Long, AtomicInteger>> iterator = entry.getValue().entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<Long, AtomicInteger> statEntry = iterator.next();
                        if (statEntry.getKey() < curStatKey) {
                            iterator.remove();
                        }
                    }
                    if (entry.getValue().isEmpty()) {
                        emptyKey.add(entry.getKey());
                    }
                }
                //可能误删，但是不影响逻辑，不加锁
                for (String key : emptyKey) {
                    metricStat.remove(key);
                }
            } catch (Exception e) {
                logger.error("", e);
            }
        }
    }

    protected boolean isNeedLimit(String topic, long time, long cacheWindowSize, int limitTimes) {
        //有线程可能正在清理，判断会有失误，但是不加锁进行处理了
        ConcurrentMap<Long, AtomicInteger> stat = new ConcurrentHashMap<Long, AtomicInteger>();
        ConcurrentMap<Long, AtomicInteger> oldStat = metricStat.putIfAbsent(topic , stat);
        if (oldStat != null) {
            stat = oldStat;
        }
        Long statWind = getStatKey(time, cacheWindowSize);
        AtomicInteger times = new AtomicInteger(0);
        AtomicInteger oldTimes = stat.putIfAbsent(statWind, times);
        if (oldTimes != null) {
            times = oldTimes;
        } else {
            //清除不用数据KEY
            Iterator<Map.Entry<Long, AtomicInteger>> iterator = stat.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, AtomicInteger> statEntry = iterator.next();
                if (statEntry.getKey() < statWind) {
                    iterator.remove();
                }
            }
        }
        int curTimes = times.incrementAndGet();
        return curTimes > limitTimes;
    }

    private Long getStatKey(long now, long windSize) {
        return now / windSize;
    }
}