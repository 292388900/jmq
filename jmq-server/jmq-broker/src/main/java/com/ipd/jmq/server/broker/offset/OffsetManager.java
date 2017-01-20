package com.ipd.jmq.server.broker.offset;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ipd.jmq.common.cluster.OffsetItem;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.MessageLocation;
import com.ipd.jmq.common.message.MessageQueue;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.store.JMQStore;
import com.ipd.jmq.server.store.Store;
import com.ipd.jmq.toolkit.io.Files;
import com.ipd.jmq.toolkit.lang.Charsets;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.service.ServiceThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 偏移量管理器
 */
public class OffsetManager extends Service {
    private static Logger logger = LoggerFactory.getLogger(OffsetManager.class);
    // 队列信号量
    private final Object queueMutex = new Object();
    // 位置信号量
    private final Object offsetMutex = new Object();
    // 偏移量文件
    private File offsetFile;
    // 队列文件
    private File queueFile;
    // 偏移量文件备份(双写)
    private File offsetFileBack;
    // 队列文件备份(双写)
    private File queueFileBack;
    // 存储配置
    private OffsetConfig config;
    // 消费者偏移量
    private ConcurrentMap<String, TopicOffset> offsets = new ConcurrentHashMap<String, TopicOffset>();
    // 队列
    private ConcurrentMap<String, Short> queues = new ConcurrentHashMap<String, Short>();
    // 存储服务
    private Store store;
    // 检查点线程
    private Thread thread;

    private ClusterManager clusterManager;

    // 消费位置复制管理器
    private OffsetReplication replication;


    public OffsetManager(OffsetConfig config) {
        Preconditions.checkArgument(config != null, "config can not be null");
        this.config = config;
        this.offsetFile = config.getOffsetFile();
        this.queueFile = config.getTopicQueueFile();
        this.offsetFileBack =
                new File(this.offsetFile.getParentFile(), this.offsetFile.getName() + JMQStore.BACK_SUFFIX);
        this.queueFileBack = new File(this.queueFile.getParentFile(), this.queueFile.getName() + JMQStore.BACK_SUFFIX);
    }

    @Override
    protected void validate() throws Exception {
        super.validate();
        Preconditions.checkArgument(store != null, "store can not be null");
        Preconditions.checkArgument(clusterManager != null, "cluster manager can not be null");
        Files.createFile(offsetFile);
        Files.createFile(queueFile);
        Files.createFile(offsetFileBack);
        Files.createFile(queueFileBack);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        try {
            offsets = loadFromFile(offsetFile, new TypeReference<ConcurrentMap<String, TopicOffset>>() {
            });
        } catch (Exception e) {
            offsets = loadFromFile(offsetFileBack, new TypeReference<ConcurrentMap<String, TopicOffset>>() {
            });
        }

        if (offsets == null) {
            offsets = new ConcurrentHashMap<String, TopicOffset>();
        }
        try {
            queues = loadFromFile(queueFile, new TypeReference<ConcurrentMap<String, Short>>() {
            });
        } catch (Exception e) {
            queues = loadFromFile(queueFileBack, new TypeReference<ConcurrentMap<String, Short>>() {
            });
        }
        if (queues == null) {
            queues = new ConcurrentHashMap<String, Short>();
        }

        // 更新信息
        TopicOffset topicOffset;
        QueueOffset queueOffset;
        UnSequenceOffset offset;
        for (Map.Entry<String, TopicOffset> topicEntry : offsets.entrySet()) {
            topicOffset = topicEntry.getValue();
            topicOffset.setTopic(topicEntry.getKey());
            for (Map.Entry<Integer, QueueOffset> queueEntry : topicOffset.getOffsets().entrySet()) {
                queueOffset = queueEntry.getValue();
                queueOffset.setTopic(topicOffset.getTopic());
                queueOffset.setQueueId(queueEntry.getKey());
                for (Map.Entry<String, UnSequenceOffset> appEntry : queueOffset.getOffsets().entrySet()) {
                    offset = appEntry.getValue();
                    offset.setTopic(topicOffset.getTopic());
                    offset.setQueueId((short) queueOffset.getQueueId());
                    offset.setConsumer(appEntry.getKey());
                    offset.setPullEndOffset(offset.getAckOffset());
                }
            }
        }

        replication = new OffsetReplication(clusterManager, this);
        replication.start();

        thread = new Thread(new ServiceThread(this) {
            @Override
            protected void execute() throws Exception {
                flushOffset();
            }

            @Override
            public long getInterval() {
                return 1000 * 30;
            }

            @Override
            public boolean onException(Throwable e) {
                logger.error(e.getMessage(), e);
                return true;
            }


        }, "JMQ_SERVER_OFFSET_CHECK_POINT");
        thread.start();


        logger.info("offset manager is started.");
    }

    // 以线程安全的方式clone一份深拷贝
    protected ConcurrentMap<String, TopicOffset> cloneOffsets() throws CloneNotSupportedException {
        ConcurrentMap<String, TopicOffset> copyOfOffsets = new ConcurrentHashMap<String, TopicOffset>(offsets.size());
        for (Map.Entry<String, TopicOffset> entry : offsets.entrySet()) {
            copyOfOffsets.put(entry.getKey(), entry.getValue().clone());
        }
        return copyOfOffsets;
    }

    @Override
    protected void doStop() {
        // 不能清空offsets和offsets，否则会有并发问题
        doFlushOffset();
        doFlushTopicQueues();
        if (thread != null){
            thread.interrupt();
        }
        super.doStop();
        logger.info("offset manager is stopped.");
    }

    public void setStore(Store store) {
        this.store = store;
    }

    public void setClusterManager(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    /**
     * 从文件读取数据
     *
     * @param file          文件
     * @param typeReference 对象引用
     * @param <T>           泛型
     * @return 数据对象
     * @throws IOException
     */
    protected <T> T loadFromFile(File file, TypeReference<T> typeReference) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), Charsets.UTF_8));
        try {
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line).append('\n');
            }
            if (builder.length() > 0) {
                return JSON.parseObject(builder.toString(), typeReference);
            } else {
                return null;
            }
        } finally {
            Close.close(reader);
        }
    }

    /**
     * 输出JSON到文件
     *
     * @param file    文件
     * @param content 内容
     * @throws IOException
     */
    protected void writeFile(File file, String content) throws IOException {
        FileWriter writer = new FileWriter(file);
        try {
            writer.write(content);
            writer.flush();
        } finally {
            Close.close(writer);
        }
    }

    /**
     * 获取消费位置信息
     *
     * @return 消费位置信息
     */
    public String getConsumeOffset() {
        try {
            return JSON.toJSONString(cloneOffsets());
        } catch (CloneNotSupportedException e) {
            logger.error("getConsumeOffset failed: ", e);
        }
        return "";
    }

    /**
     * 检查点调用，定时刷新偏移量到磁盘
     */
    public void flushOffset() {
        checkState();
        doFlushOffset();
    }

    /**
     * 刷新偏移量到磁盘
     */
    protected void doFlushOffset() {
        try {
            ConcurrentMap<String, TopicOffset> copyOfoffsets = cloneOffsets();
            String jsonStr = JSON.toJSONString(copyOfoffsets);
            synchronized (offsetMutex) {
                writeFile(offsetFile, jsonStr);
                writeFile(offsetFileBack, jsonStr);
            }
        } catch (Exception e) {
            logger.error("flush offset error.", e);
        }
    }

    /**
     * 刷新主题队列数到磁盘
     */
    protected void doFlushTopicQueues() {
        try {
            String jsonStr = JSON.toJSONString(queues);
            writeFile(queueFile, jsonStr);
            writeFile(queueFileBack, jsonStr);
        } catch (Exception e) {
            logger.error("flush queues error.", e);
        }
    }

    /**
     * 确认位置为当前索引结束，下一个索引开始的位置，
     *
     * @param location 位置
     */
    public void acknowledge(final String consumer, final MessageLocation location) {
        if (location == null || consumer == null) {
            return;
        }
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("acknowledge topic:%s app:%s queueId:%d offset%d", location.getTopic(), consumer,
                    location.getQueueId(), location.getQueueOffset()));
        }
        TopicOffset topicOffset = getAndCreateOffset(location.getTopic());
        topicOffset.acknowledge(location.getQueueId(), consumer, location.getQueueOffset());

        replicate(consumer, location);
    }

    //复制消费位置
    private void replicate(String consumer, final MessageLocation location){
        OffsetItem item = new OffsetItem();
        item.setTopic(location.getTopic());
        item.setApp(consumer);
        item.setQueueOffset(location.getQueueOffset());
        item.setQueueId(location.getQueueId());
        item.setJournalOffset(location.getJournalOffset());
        replication.replicate(item);

    }

    /**
     * 重置消费位置，
     *
     * @param topic    主题
     * @param queueId  队列
     * @param consumer 消费者
     * @param offset   偏移量
     */
    public void resetAckOffset(final String topic, final int queueId, final String consumer, final long offset) {
        resetAckOffset(topic, queueId, consumer, offset, true);
    }

    /**
     * 重置消费位置
     *
     * @param topic    主题
     * @param queueId  队列
     * @param consumer 消费者
     * @param offset   偏移量
     * @param replicate   是否复制到其他Broker
     */
    public void resetAckOffset(final String topic, final int queueId, final String consumer, final long offset,
                               boolean replicate) {
        if (logger.isInfoEnabled()) {
            logger.info(
                    String.format("reset topic:%s,queueId:%d consumer:%s,offset %d", topic, queueId, consumer, offset));
        }
        TopicOffset topicOffset = getAndCreateOffset(topic);
        topicOffset.resetAckOffset(queueId, consumer, offset);

        if (replicate) {
            // 复制到复制到其他broker
            OffsetItem item = new OffsetItem();
            item.setTopic(topic);
            item.setApp(consumer);
            item.setQueueOffset(offset);
            item.setQueueId((short) queueId);
            replication.replicate(item);
        }
    }

    /**
     * 获取偏移量，不存在则创建
     *
     * @param topic 主题
     * @return 偏移量
     */
    protected TopicOffset getAndCreateOffset(final String topic) {
        TopicOffset result = offsets.get(topic);
        if (result == null) {
            result = new TopicOffset(topic);
            TopicOffset old = offsets.putIfAbsent(topic, result);
            if (old != null) {
                result = old;
            }
        }
        return result;
    }

    /**
     * 获取偏移量
     *
     * @param topic 主题
     * @return 偏移量
     */
    public TopicOffset getOffset(final String topic) {
        return offsets.get(topic);
    }

    public Map<String, TopicOffset> getOffset() {
        return new ConcurrentHashMap<>(offsets);
    }


    /**
     * 指定主题是否有订阅者
     *
     * @param topic 主题
     * @return 有订阅者标示
     */
    public boolean hasConsumer(final String topic) {
        TopicOffset topicOffset = getOffset(topic);
        if (topicOffset == null) {
            return false;
        }
        return topicOffset.hasConsumer();
    }


    /**
     * 获取消费位置
     *
     * @param topic    主题
     * @param queueId  队列ID
     * @param consumer 消费者
     */
    public UnSequenceOffset getOffset(final String topic, final int queueId, final String consumer) {
        if (topic == null || topic.isEmpty() || consumer == null || consumer
                .isEmpty() || queueId == MessageQueue.RETRY_QUEUE) {
            return null;
        }
        TopicOffset topicOffset = this.getOffset(topic);
        if (topicOffset != null) {
            QueueOffset queueOffset = topicOffset.getOffset(queueId);
            if (queueOffset != null) {
                UnSequenceOffset offset = queueOffset.getOffset(consumer);
                return offset;
            }
        }
        return null;
    }

    /**
     * 获取最小的应答位置
     *
     * @param topic          主题
     * @param queueId        队列ID
     * @param minQueueOffset 最小偏移量
     * @return 该队列的最小位置
     */
    public long getMinAckOffset(final String topic, final int queueId, long minQueueOffset) {
        if (topic == null || topic.isEmpty()) {
            return minQueueOffset;
        }
        long result = -1;
        TopicOffset topicOffset = offsets.get(topic);
        if (topicOffset != null) {
            QueueOffset queueOffset = topicOffset.getOffset(queueId);
            if (queueOffset != null) {
                for (Offset offset : queueOffset.getOffsets().values()) {
                    long ack = offset.getAckOffset().get();
                    // 判断是否有效
                    if (ack > minQueueOffset) {
                        if (result == -1) {
                            result = ack;
                        } else {
                            result = ack < result ? ack : result;
                        }
                    }
                }
            }
        }
        return result;
    }

    /**
     * 变更了队列，需要更新订阅关系和记录位置
     *
     * @param topic  主题
     * @param queues 当前队列
     */
    public void addConsumer(final String topic, final List<Integer> queues) {
        rwLock.readLock().lock();
        try {
            checkState();
            doAddConsumer(topic, queues);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * 变更了队列，需要更新订阅关系和记录位置
     *
     * @param topic  主题
     * @param queues 当前队列
     */
    protected void doAddConsumer(final String topic, final List<Integer> queues) {
        if (queues == null || topic == null || topic.isEmpty()) {
            return;
        }
        synchronized (offsetMutex) {
            TopicOffset topicOffset = getAndCreateOffset(topic);
            Iterator<Integer> iterator = queues.iterator();
            while (iterator.hasNext()) {
                topicOffset.getAndCreateOffset(iterator.next());
            }
            // 获取订阅关系
            Set<String> consumers = topicOffset.getConsumers().keySet();
            if (!consumers.isEmpty()) {
                boolean flag = false;
                // 遍历消费者，创建订阅位置
                for (String consumer : consumers) {
                    if (createOffset(topicOffset, consumer, queues)) {
                        flag = true;
                    }
                }
                if (flag) {
                    doFlushOffset();
                }
            }
        }
    }

    protected void checkState() {
        if (!isStarted()) {
            throw new IllegalStateException("offset manager was stopped");
        }
    }

    /**
     * 新增订阅者，用于记录第一次订阅时开始的位置
     *
     * @param topic    主题
     * @param consumer 消费者
     */
    public void addConsumer(final String topic, final String consumer) {
        if (topic == null || topic.isEmpty() || consumer == null || consumer.isEmpty()) {
            return;
        }
        checkState();
        //TODO 是否应该直接从存储获取此队列
        List<Integer> queues = store.getQueues(topic);
        TopicOffset topicOffset = getAndCreateOffset(topic);
        topicOffset.addConsumer(consumer);
        boolean flag = createOffset(topicOffset, consumer, queues);
        if (flag) {
            doFlushOffset();
        }
    }

    /**
     * 创建消费者位置偏移量
     *
     * @param topicOffset 主题偏移量
     * @param consumer    消费者
     * @param queues      队列
     * @return 成功
     */
    protected boolean createOffset(final TopicOffset topicOffset, final String consumer,
                                   final List<Integer> queues) {
        boolean flag = false;
        QueueOffset queueOffset;
        Offset offset;
        AtomicLong subscribeOffset;
        AtomicLong ackOffset;
        for (Integer queueId : queues) {
            queueOffset = topicOffset.getAndCreateOffset(queueId);
            if (!queueOffset.getOffsets().containsKey(consumer)) {
                //消费者信息不存在才订阅，否则不需要重复订阅
                offset = queueOffset.getAndCreateOffset(consumer);
                subscribeOffset = offset.getSubscribeOffset();
                if (subscribeOffset.compareAndSet(0, store.getMaxOffset(offset.getTopic(), queueId))) {
                    ackOffset = offset.getAckOffset();
                    ackOffset.compareAndSet(0, subscribeOffset.get());
                    flag = true;
                }
            }
        }
        return flag;
    }

    /**
     * 移除订阅者
     *
     * @param topic    主题
     * @param consumer 消费者
     */
    public void removeConsumer(final String topic, final String consumer) {
        if (topic == null || topic.isEmpty() || consumer == null || consumer.isEmpty()) {
            return;
        }
        checkState();
        TopicOffset topicOffset = getOffset(topic);
        if (topicOffset == null) {
            return;
        }
        topicOffset.removeConsumer(consumer);
        boolean flag = false;
        QueueOffset queueOffset;
        Offset offset;
        for (Map.Entry<Integer, QueueOffset> e : topicOffset.getOffsets().entrySet()) {
            queueOffset = e.getValue();
            offset = queueOffset.getOffsets().remove(consumer);
            if (offset != null) {
                flag = true;
            }
        }
        if (flag) {
            doFlushOffset();
        }
    }

    /**
     * 普通队列数
     *
     * @param topic 主题
     * @return 队列数
     */
    public short getQueueCount(final String topic) {
        Short count = queues.get(topic);
        return count == null ? 0 : count.shortValue();
    }

    /**
     * 设置队列数
     *
     * @param topic 主题
     * @param count 数量
     * @return 原来的队列数量
     */
    public short updateQueueCount(final String topic, final short count)throws JMQException {
        // 对同一个主题不能并发调用。外面调用主要是单线程，并发量很小
        checkState();
        doAddConsumer(topic, store.updateQueues(topic, count));
        return doUpdateQueueCount(topic, count);
    }

    protected short doUpdateQueueCount(final String topic, final short count) {
        if (topic == null || topic.isEmpty()) {
            return 0;
        }
        synchronized (queueMutex) {
            Short old = queues.get(topic);
            if (old == null || old != count) {
                queues.put(topic, count);
                try {
                    doFlushTopicQueues();
                } catch (Exception e) {
                    logger.error("flush queues error.", e);
                }
            }
            return old == null ? 0 : old;
        }
    }

    /**
     * 合并数据
     *
     * @param others 其它偏移量
     */
    public void updateOffset(final Map<String, TopicOffset> others) {
        if (others == null || others.isEmpty()) {
            return;
        }
        checkState();
        for (Map.Entry<String, TopicOffset> entry : others.entrySet()) {
            TopicOffset target = getAndCreateOffset(entry.getKey());
            target.updateOffset(entry.getValue());
        }
        doFlushOffset();
    }
}