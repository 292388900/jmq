package com.ipd.jmq.common.cluster;

import com.ipd.jmq.common.model.*;
import com.ipd.jmq.toolkit.retry.RetryPolicy;

import java.io.Serializable;
import java.util.*;

/**
 * 从zookeeper上获取topic对应哪些brokerCluster,哪些消费者，哪些生产者。
 * User: weiqisong
 * Date: 14-4-21
 * Time: 上午10:35
 */
public class TopicConfig implements Serializable {
    public final static int TopicVeryImportant = 0;
    public final static int TopicImportant = 1;
    public final static int TopicGeneral = 2;

    private static final long serialVersionUID = 518558108264597599L;
    public static final char APP_NAME_SPLITOR = '.';
    // 主题
    private String topic;
    // 重要性 (0=非常重要，1=重要，2=一般), null=一般
    private Integer importance;
    // 分组
    private Set<String> groups = new HashSet<String>();
    // 生产者信息
    private Map<String, ProducerPolicy> producers = new HashMap<String, ProducerPolicy>();
    // 消费者信息
    private Map<String, ConsumerPolicy> consumers = new HashMap<String, ConsumerPolicy>();
    // 是否需要归档。默认不需要归档
    private boolean archive = false;
    // 需要队列的大小
    private short queues;
    // 类型
    private TopicType type;

    public TopicConfig() {
    }

    public TopicConfig(String topic, short queues, boolean archive) {
        this.topic = topic;
        this.queues = queues;
        this.archive = archive;
    }

    /**
     * 获取配置的app名
     *
     * @param srcApp 原始APP
     * @return 配置的APP
     */
    public static String getApp(final String srcApp) {
        if (srcApp != null) {
            int pos = srcApp.lastIndexOf(APP_NAME_SPLITOR);
            if (pos > 0) {
                return srcApp.substring(0, pos);
            }
        }
        return srcApp;
    }

    /**
     * 主题配置转换
     *
     * @param topic 主题
     * @return 主题配置
     */
    public static TopicConfig toTopicConfig(Topic topic) {
        if (topic == null) {
            return null;
        }
        TopicConfig config = new TopicConfig();
        config.setTopic(topic.getCode());
        config.setArchive(topic.isArchive());
        config.setQueues((short) topic.getQueues());
        config.setType(TopicType.valueOf(topic.getType()));
//        config.setImportance(topic.getImportance());
        return config;
    }

    /**
     * 根据数据库配置转换成消费策略
     *
     * @param topic    主题
     * @param consumer 消费配置
     * @return 消费策略
     */
    public static ConsumerPolicy toPolicy(Topic topic, Consumer consumer) {
        if (consumer == null) {
            return null;
        }
        ConsumerPolicy consumerPolicy = new ConsumerPolicy();
        consumerPolicy.setAckTimeout(consumer.getAckTimeout() <= 0 ? null : consumer.getAckTimeout());
        consumerPolicy.setArchive(topic.isArchive() && !consumer.isArchive() ? false : null);
        consumerPolicy.setBatchSize(consumer.getBatchSize() <= 0 ? null : (short) consumer.getBatchSize());
        consumerPolicy.setNearby(consumer.isNearby() ? true : null);
        consumerPolicy.setPaused(consumer.isPaused() ? true : null);
        consumerPolicy.setRetry(consumer.isRetry() ? null : false);
        consumerPolicy.setSeq(TopicType.valueOf(topic.getType()) == TopicType.SEQUENTIAL ? true : null);
        consumerPolicy.setOffsetMode(OffsetMode.REMOTE.equals(consumer.getOffsetMode()) ? null : consumer.getOffsetMode());
        consumerPolicy.setConcurrentConsume(consumer.getConcurrents() > 0 ? true : null);
        consumerPolicy.setDelay(consumer.getDelay() > 0 ? consumer.getDelay() : null);
//        consumerPolicy.setRole(ClusterRole.MASTER.equals(consumer.getRole()) ? null : consumer.getRole());
//        consumerPolicy.setSelector(consumer.getSelector());
//        consumerPolicy.setPrefetchSize(consumer.getPrefetchSize() > 0 ? consumer.getPrefetchSize() : null);
        RetryPolicy retryPolicy = new RetryPolicy();
        retryPolicy.setRetryDelay(consumer.getRetryDelay() <= 0 ? null : consumer.getRetryDelay());
        retryPolicy.setMaxRetryDelay(consumer.getMaxRetryDelay() <= 0 ? null : consumer.getMaxRetryDelay());
        retryPolicy.setMaxRetrys(consumer.getMaxRetrys() <= 0 ? null : consumer.getMaxRetrys());
        retryPolicy.setExpireTime(consumer.getExpireTime() <= 0 ? null : consumer.getExpireTime());
        retryPolicy.setUseExponentialBackOff(consumer.isUseExpBackoff() ? null : false);
        retryPolicy.setBackOffMultiplier(consumer.getBackoffMultiplier() <= 0 ? null : consumer.getBackoffMultiplier());
        if (retryPolicy.getRetryDelay() != null || retryPolicy.getMaxRetryDelay() != null || retryPolicy
                .getMaxRetrys() != null || retryPolicy.getUseExponentialBackOff() != null || retryPolicy
                .getBackOffMultiplier() != null || retryPolicy.getExpireTime() != null) {
            consumerPolicy.setRetryPolicy(retryPolicy);
        }
        return consumerPolicy;
    }

    /**
     * 根据数据库配置转换成生产策略
     *
     * @param topic    主题
     * @param producer 生产配置
     * @return 生产策略
     */
    public static ProducerPolicy toPolicy(Topic topic, Producer producer) {
        if (producer == null) {
            return null;
        }
        ProducerPolicy policy = new ProducerPolicy();
        policy.setSeq(TopicType.valueOf(topic.getType()) == TopicType.SEQUENTIAL ? true : null);
        policy.setNearby(producer.isNearBy() ? true : null);
        policy.setWeight(producer.weights());
//        policy.setSingle(producer.isSingle() ? true : null);
//        policy.setTxTimeout(producer.getTxTimeout() > 0 ? producer.getTxTimeout() : null);
        return policy;
    }

    /**
     * 转换为主题配置
     *
     * @param topics    主题
     * @param producers 生产策略
     * @param consumers 消费策略
     * @param topicShards    分组
     * @return 主题配置列表
     * @throws Exception
     */
    public static List<TopicConfig> toTopicConfigs(List<Topic> topics, List<Producer> producers,
                                                   List<Consumer> consumers, List<TopicShard> topicShards) throws Exception {
        if (topics == null || topics.isEmpty()) {
            return new ArrayList<TopicConfig>();
        }
        Map<Long, TopicConfig> configMap = new HashMap<Long, TopicConfig>();
        Map<Long, Topic> topicMap = new HashMap<Long, Topic>();
        for (Topic topic : topics) {
            topicMap.put(topic.getId(), topic);
            configMap.put(topic.getId(), toTopicConfig(topic));
        }

        TopicConfig config;
        Topic topic;
        for (Producer producer : producers) {
            config = configMap.get(producer.getTopic().getId());
            topic = topicMap.get(producer.getTopic().getId());
            if (config != null) {
                if (producer.getApp().getCode() == null || "".equals(producer.getApp().getCode())) {
                    continue;
                }
                config.addProducerPolicy(producer.getApp().getCode(), toPolicy(topic, producer));
            }
        }
        for (Consumer consumer : consumers) {
            config = configMap.get(consumer.getTopic().getId());
            topic = topicMap.get(consumer.getTopic().getId());
            if (config != null) {
                config.addConsumerPolicy(consumer.getApp().getCode(), toPolicy(topic, consumer));
            }
        }
        for (TopicShard topicShard : topicShards) {
            config = configMap.get(topicShard.getTopic().getId());
            if (config != null) {
                config.getGroups().add(topicShard.getShard().getCode());
            }
        }
        return new ArrayList<TopicConfig>(configMap.values());
    }


    public short getQueues() {
        return queues;
    }

    public void setQueues(short queues) {
        this.queues = queues;
    }

    public boolean isArchive() {
        return archive;
    }

    public void setArchive(boolean archive) {
        this.archive = archive;
    }

    public boolean checkSequential() {
        return TopicType.SEQUENTIAL == type;
    }

    //policy 不为空的前提下默认允许多个发送者
    public boolean singleProducer(String app) {
        ProducerPolicy policy = producers.get(app);
        return policy == null ? false : (policy.getSingle() == null ? false : policy.getSingle());
    }

    public boolean isBroadcastTopic() {
        return TopicType.BROADCAST == type;
    }

    public boolean isLocalManageOffsetConsumer(String consumer) {
        //TODO 通过消费策略可以设置
        if (isBroadcastTopic() || isConsumerByLocalOffset(consumer)) {
            return true;
        }
        return false;
    }

    public boolean isConsumerByLocalOffset(String consumer) {
        if (consumers == null || consumers.isEmpty()) {
            return false;
        }
        ConsumerPolicy consumerPolicy;
        consumerPolicy = consumers.get(consumer);
        if (consumerPolicy == null) {
            return false;
        }
        if (consumerPolicy.getOffsetMode() != null && consumerPolicy.getOffsetMode() == OffsetMode.LOCAL) {
            return true;
        } else {
            return false;
        }
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getImportance() {
        return (importance == null) ? TopicGeneral : importance;
    }

    public void setImportance(Integer importance) {
        this.importance = importance;
    }

    public TopicType getType() {
        if (type == null) {
            return TopicType.TOPIC;
        }
        return type;
    }

    public void setType(TopicType type) {
        this.type = type;
    }

    public Map<String, ProducerPolicy> getProducers() {
        return producers;
    }

    public void setProducers(Map<String, ProducerPolicy> producers) {
        this.producers = producers;
    }

    public ProducerPolicy getProducerPolicy(String app) {
        return producers.get(app);
    }

    public ConsumerPolicy getConsumerPolicy(String app) {
        return consumers.get(app);
    }

    public Map<String, ConsumerPolicy> getConsumers() {
        return consumers;
    }

    public void setConsumers(Map<String, ConsumerPolicy> consumers) {
        this.consumers = consumers;
    }

    public Set<String> getGroups() {
        return groups;
    }

    public void setGroups(Set<String> groups) {
        this.groups = groups;
    }

    public void addProducerPolicy(String app, ProducerPolicy policy) {
        if (app == null || app.isEmpty()) {
            return;
        }
        producers.put(app, policy);
    }

    public void addConsumerPolicy(String app, ConsumerPolicy policy) {
        if (app == null || app.isEmpty()) {
            return;
        }
        consumers.put(app, policy);
    }

    public boolean isProducer(final String app) {
        return producers.containsKey(getApp(app));
    }

    public boolean isConsumer(final String app) {
        return consumers.containsKey(getApp(app));
    }

    public boolean containGroup(final String group) {
        return groups.contains(group);
    }

    /**
     * 是否可以生产消息
     *
     * @param producer 生产者系统
     * @return 可以生产消息标示
     */
    public boolean isWritable(final String producer) {
        return producers.containsKey(getApp(producer)) && !groups.isEmpty();
    }

    /**
     * 是否可以消费消息
     *
     * @param consumer 消费者系统
     * @return 可以消费消息标示
     */
    public boolean isReadable(final String consumer) {
        ConsumerPolicy consumerPolicy = consumers.get(getApp(consumer));
        return consumerPolicy != null && !consumerPolicy.isPaused() && !groups.isEmpty();
    }

    /**
     * 是否开启归档
     *
     * @param consumer 消费者
     * @return 归档标示
     */
    public boolean isArchive(final String consumer) {
        ConsumerPolicy consumerPolicy = consumers.get(getApp(consumer));
        if (consumerPolicy != null) {
            return isArchive() && (consumerPolicy.getArchive() == null || consumerPolicy.getArchive());
        } else {
            return isArchive();
        }
    }

    /**
     * 主题类型
     */
    public static enum TopicType implements Serializable {
        /**
         * 主题
         */
        TOPIC,
        /**
         * 广播
         */
        BROADCAST,
        /**
         * 顺序队列
         */
        SEQUENTIAL;

        public static TopicType valueOf(final int value) {
            switch (value) {
                case 0:
                    return TOPIC;
                case 1:
                    return BROADCAST;
                case 2:
                    return SEQUENTIAL;
                default:
                    return TOPIC;

            }
        }
    }

    /**
     * 生产者策略
     */
    public static class ProducerPolicy implements Serializable {
        // 应用
        private transient String app;
        // 就近发送
        private Boolean nearby;
        // 是否顺序
        private Boolean seq;
        // 是否允许多个发送者
        @Deprecated
        private Boolean single;
        // 生产者权重 <group,weight>
        private Map<String, Short> weight;
        //
        @Deprecated
        private Integer txTimeout;

        public ProducerPolicy() {
        }

        public ProducerPolicy(String app) {
            this.app = app;
        }

        public String getApp() {
            return app;
        }

        public boolean isNearby() {
            return nearby != null && nearby;
        }

        public Boolean getNearby() {
            return nearby;
        }

        public void setNearby(Boolean nearby) {
            this.nearby = nearby;
        }

        public Map<String, Short> getWeight() {
            return weight;
        }

        public void setWeight(Map<String, Short> weight) {
            this.weight = weight;
        }

        public void setApp(String app) {
            this.app = app;
        }

        public Boolean getSeq() {
            return seq;
        }

        public void setSeq(Boolean seq) {
            this.seq = seq;
        }

        public Boolean getSingle() {
            return single;
        }

        public void setSingle(Boolean single) {
            this.single = single;
        }

        public Integer getTxTimeout() {
            return txTimeout;
        }

        public void setTxTimeout(Integer txTimeout) {
            this.txTimeout = txTimeout;
        }
    }

    /**
     * 消费策略
     */
    public static class ConsumerPolicy implements Serializable {
        // 应用
        private transient String app;
        // 就近发送
        private Boolean nearby;
        // 是否暂停消费
        private Boolean paused;
        // 是否需要归档,默认归档
        private Boolean archive;
        // 是否需要重试，默认重试
        private Boolean retry;
        // 顺序消费
        private Boolean seq;
        // 过滤器
        @Deprecated
        private String selector;
        // 消费节点角色 null表示master
        @Deprecated
        private ClusterRole role;
        // 消息偏移量管理 null 表示remote
        private OffsetMode offsetMode;
        // 重试策略
        private RetryPolicy retryPolicy;
        // 应答超时时间
        private Integer ackTimeout;
        // 批量大小
        private Short batchSize;
        //预取大小
        private Integer prefetchSize;
        //并行消费
        private Boolean concurrentConsume;
        //延迟消费
        private Integer delay;

        public ConsumerPolicy() {
        }

        public ConsumerPolicy(String app) {
            this.app = app;
        }

        public String getApp() {
            return app;
        }

        public boolean isNearby() {
            return nearby != null && nearby;
        }

        public Boolean getNearby() {
            return nearby;
        }

        public void setNearby(Boolean nearby) {
            this.nearby = nearby;
        }

        public boolean isPaused() {
            return paused != null && paused;
        }

        public Boolean getPaused() {
            return paused;
        }

        public void setPaused(Boolean paused) {
            this.paused = paused;
        }

        public Boolean getArchive() {
            return archive;
        }

        public void setArchive(Boolean archive) {
            this.archive = archive;
        }

        public boolean isRetry() {
            return retry == null || retry;
        }

        public Boolean getRetry() {
            return retry;
        }

        public void setRetry(Boolean retry) {
            this.retry = retry;
        }


        public boolean checkSequential() {
            return seq != null && seq;
        }

        public Boolean getSeq() {
            return seq;
        }

        public void setSeq(Boolean seq) {
            this.seq = seq;
        }

        public String getSelector() {
            return selector;
        }

        public void setSelector(String selector) {
            this.selector = selector;
        }

        public ClusterRole getRole() {
            return role;
        }

        public void setRole(ClusterRole role) {
            this.role = role;
        }

        public OffsetMode getOffsetMode() {
            return offsetMode;
        }

        public void setOffsetMode(OffsetMode offsetMode) {
            this.offsetMode = offsetMode;
        }

        public RetryPolicy getRetryPolicy() {
            return retryPolicy;
        }

        public void setRetryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
        }

        public Integer getAckTimeout() {
            return ackTimeout;
        }

        public void setAckTimeout(Integer ackTimeout) {
            this.ackTimeout = ackTimeout;
        }

        public Short getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(Short batchSize) {
            this.batchSize = batchSize;
        }

        public Integer getPrefetchSize() {
            return prefetchSize;
        }

        public void setPrefetchSize(Integer prefetchSize) {
            this.prefetchSize = prefetchSize;
        }

        public Boolean getConcurrentConsume() {
            return concurrentConsume;
        }

        public void setConcurrentConsume(Boolean concurrentConsume) {
            this.concurrentConsume = concurrentConsume;
        }

        public Integer getDelay() {
            return delay;
        }

        public void setDelay(Integer delay) {
            this.delay = delay;
        }
    }
}
