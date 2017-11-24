package com.ipd.jmq.server.store;

import com.ipd.jmq.common.message.MessageQueue;
import com.ipd.jmq.common.message.QueueItem;
import com.ipd.jmq.server.store.journal.AppendFileManager;
import com.ipd.jmq.server.store.journal.FileStat;
import com.ipd.jmq.server.store.journal.JournalConfig;
import com.ipd.jmq.server.store.journal.VersionHeader;
import com.ipd.jmq.toolkit.io.Files;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.validate.Validators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 队列文件管理器
 *
 * @author dingjun
 */
public class QueueManager extends Service {
    private static final String QUEUE_MAPPED_MANAGER = "JMQ_queueMappedFileManager";
    private static Logger logger = LoggerFactory.getLogger(QueueManager.class);
    //日志文件配置
    private JournalConfig journalConfig;
    // 存储配置
    private StoreConfig config;
    // 队列目录
    private File queueDirectory;
    // 队列
    private ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> queues =
            new ConcurrentHashMap<String, ConcurrentMap<Integer, ConsumeQueue>>();
    // 追加文件管理器
    private AppendFileManager appendFileManager;

    /**
     * 构造函数
     *
     * @param config 存储配置
     */
    public QueueManager(StoreConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config can not be null.");
        }
        this.journalConfig = config.getQueueConfig();
        if (journalConfig == null){
            throw new IllegalArgumentException("journalConfig can not be null.");
        }
        if ( journalConfig.getDataSize()> 0) {
            if (journalConfig.getDataSize() % ConsumeQueue.CQ_RECORD_SIZE != 0) {
                throw new IllegalArgumentException(
                        String.format("queueFileSize must be a multiple of %d.", ConsumeQueue.CQ_RECORD_SIZE));
            }
        }

        this.config = config;
        this.queueDirectory = config.getQueueDirectory();
    }

    @Override
    protected void validate() throws Exception {
        super.validate();
        Validators.validate(journalConfig);
        Files.createDirectory(queueDirectory);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        appendFileManager = new AppendFileManager(journalConfig, new VersionHeader(), QUEUE_MAPPED_MANAGER);
        appendFileManager.start();
        // 遍历目录，创建队列
        File[] topicDirs = queueDirectory.listFiles();
        if (topicDirs != null) {
            // 遍历Topic目录
            for (File topicDir : topicDirs) {
                if (!topicDir.isDirectory()) {
                    continue;
                }
                ConcurrentMap<Integer, ConsumeQueue> queues = new ConcurrentHashMap<Integer, ConsumeQueue>();
                String topic = topicDir.getName();
                this.queues.put(topic, queues);
                File[] queueDirs = topicDir.listFiles();
                if (queueDirs != null) {
                    // 遍历该Topic下的队列目录
                    for (File queueDir : queueDirs) {
                        if (!queueDir.isDirectory()) {
                            continue;
                        }
                        try {
                            Integer queueId = Integer.valueOf(queueDir.getName());
                            ConsumeQueue queue = new ConsumeQueue(topic, queueId, queueDir, appendFileManager, config);
                            queue.start();
                            queues.put(queueId, queue);
                        } catch (NumberFormatException ignored) {
                            // 过滤掉错误的目录
                        }
                    }
                }
            }
        }
        logger.info("queue manager is started");
    }

    @Override
    protected void beforeStop() {
        super.beforeStop();
        // 将要关闭，通知消费队列，避免长时间恢复
        for (ConcurrentMap<Integer, ConsumeQueue> consumeQueues : queues.values()) {
            for (ConsumeQueue consumeQueue : consumeQueues.values()) {
                consumeQueue.willStop();
            }
        }
    }

    @Override
    protected void doStop() {
        for (ConcurrentMap<Integer, ConsumeQueue> consumeQueues : queues.values()) {
            for (ConsumeQueue consumeQueue : consumeQueues.values()) {
                // stop会调用flush方法
                consumeQueue.stop();
            }
        }
        if (appendFileManager != null) {
            appendFileManager.stop();
        }
        queues.clear();
        logger.info("queue manager is stopped.");
    }

    /**
     * 把所有队列刷盘
     *
     * @throws IOException
     */
    public void flush() throws IOException {
        for (ConcurrentMap<Integer, ConsumeQueue> consumeQueues : queues.values()) {
            for (ConsumeQueue consumeQueue : consumeQueues.values()) {
                consumeQueue.flush();
                await(config.getQueueFlushFileInterval());
            }
        }
    }

    /**
     * 增加一个主题
     *
     * @param topic      主题
     * @param queueCount 队列数量
     * @throws Exception
     */
    public List<Integer> addTopic(final String topic, final int queueCount) throws Exception {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic can not be empty");
        }
        if (queueCount <= 0 || queueCount > MessageQueue.MAX_NORMAL_QUEUE) {
            throw new IllegalArgumentException("queueCount must be in [1," + MessageQueue.MAX_NORMAL_QUEUE + "]");
        }
        // 获取队列，可能包含高级队列，如果比较普通队列数，需要排除
        ConcurrentMap<Integer, ConsumeQueue> consumeQueues = getAndCreateTopic(topic);
        // 创建队列
        List<Integer> queues = new ArrayList<>();
        for (short i = 1; i <= queueCount; i++) {
            getAndCreateQueue(topic, i, consumeQueues);
            queues.add((int) i);
        }
        return queues;
    }

    /**
     * 创建主题的消费队列
     *
     * @param topic 主题
     * @return 该主题的消费队列
     */
    protected ConcurrentMap<Integer, ConsumeQueue> getAndCreateTopic(String topic) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic can not be empty");
        }
        // 根据主题查找
        ConcurrentMap<Integer, ConsumeQueue> consumerQueues = queues.get(topic);
        if (consumerQueues == null) {
            consumerQueues = new ConcurrentHashMap<Integer, ConsumeQueue>();
            ConcurrentMap<Integer, ConsumeQueue> old = queues.putIfAbsent(topic, consumerQueues);
            if (old != null) {
                consumerQueues = old;
            }
        }
        return consumerQueues;
    }

    /**
     * 返回消费队列，不存在则创建
     *
     * @param topic   主题
     * @param queueId 队列ID
     * @throws Exception
     */
    public ConsumeQueue getAndCreateQueue(final String topic, final short queueId) throws Exception {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic can not be empty");
        }
        if (queueId < 0 || queueId > MessageQueue.MAX_NORMAL_QUEUE && queueId != MessageQueue.HIGH_PRIORITY_QUEUE) {
            throw new IllegalArgumentException("queueId is invalid,queue:" + queueId);
        }
        return getAndCreateQueue(topic, queueId, getAndCreateTopic(topic));
    }

/**
     * 返回消费队列，不存在则创建
     *
     * @param topic   主题
     * @param queueId 队列ID
     * @throws Exception
     */
    public ConsumeQueue getAndCreateQueue(final String topic, final short queueId, final long queueOffset) throws
            Exception {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic can not be empty");
        }
        if (queueId < 0 || queueId > MessageQueue.MAX_NORMAL_QUEUE && queueId != MessageQueue.HIGH_PRIORITY_QUEUE) {
            throw new IllegalArgumentException("queueId is invalid,queue:" + queueId);
        }
        return getAndCreateQueue(topic, queueId, getAndCreateTopic(topic), queueOffset);
    }

    /**
     * 返回消费队列，不存在则创建
     *
     * @param topic          主题
     * @param queueId        队列ID
     * @param consumerQueues 队列散列
     * @return 消费队列
     * @throws Exception
     */
    protected ConsumeQueue getAndCreateQueue(final String topic, final short queueId,
                                             final ConcurrentMap<Integer, ConsumeQueue> consumerQueues)
            throws Exception {
      return getAndCreateQueue(topic, queueId, consumerQueues, 0);
    }

    /**
     * 返回消费队列，不存在则创建
     *
     * @param topic          主题
     * @param queueId        队列ID
     * @param consumerQueues 队列散列
     * @param queueOffset    队列偏移位置
     *
     * @return 消费队列
     * @throws Exception
     */
    protected ConsumeQueue getAndCreateQueue(final String topic, final int queueId,
                                             final ConcurrentMap<Integer, ConsumeQueue> consumerQueues, long queueOffset)
            throws Exception {
        // 根据队列ID查找
        ConsumeQueue consumeQueue = consumerQueues.get(queueId);
        if (consumeQueue == null) {
            File topicDir = new File(queueDirectory, topic + "/");
            File queueDir = new File(topicDir, String.valueOf(queueId) + "/");
            // 启动的时候会自动创建目录
            consumeQueue = new ConsumeQueue(topic, queueId, queueDir, appendFileManager, config, queueOffset);
            ConsumeQueue old = consumerQueues.putIfAbsent(queueId, consumeQueue);
            if (old != null) {
                consumeQueue = old;
            } else {
                consumeQueue.start();
            }
        }

        return consumeQueue;
    }


    /**
     * 获取消费队列
     *
     * @param topic   主题
     * @param queueId 队列ID
     */
    public ConsumeQueue getConsumeQueue(final String topic, final int queueId) {
        ConsumeQueue consumeQueue = null;
        ConcurrentMap<Integer, ConsumeQueue> consumeQueues = queues.get(topic);
        if (consumeQueues != null) {
            consumeQueue = consumeQueues.get(queueId);
        }
        return consumeQueue;
    }

    public File getQueueDirectory() {
        return queueDirectory;
    }

    /**
     * 获取最大队列偏移量
     *
     * @param topic   主题
     * @param queueId 队列ID
     * @return 最大队列偏移量
     */
    public long getMaxOffset(final String topic, final int queueId) {
        ConsumeQueue consumeQueue = getConsumeQueue(topic, queueId);
        return consumeQueue == null ? 0 : consumeQueue.getMaxOffset();
    }

    /**
     * 获取最小队列偏移量
     *
     * @param topic   主题
     * @param queueId 队列ID
     * @return 最小队列偏移量
     */
    public long getMinOffset(final String topic, final int queueId) {
        ConsumeQueue consumeQueue = getConsumeQueue(topic, queueId);
        return consumeQueue == null ? 0 : consumeQueue.getMinOffset();
    }

    /**
     * 获取下一个消费位置
     *
     * @param topic   主题
     * @param queueId 队列ID
     * @param offset  当前偏移量
     * @return 下一个消费位置
     */
    public long getNextOffset(String topic, int queueId, long offset) {
        ConsumeQueue consumeQueue = getConsumeQueue(topic, queueId);
        return consumeQueue == null ? 0 : consumeQueue.getNextOffset(offset);
    }

    /**
     * 获取队列数据
     *
     * @param topic   主题
     * @param queueId 队列ID
     * @param offset  偏移量
     * @param count   数量
     * @return 队列数据列表
     * @throws IOException
     */
    public List<QueueItem> getQueueItem(String topic, int queueId, long offset, int count) throws IOException {
        ConsumeQueue cq = getConsumeQueue(topic, queueId);
        if (cq != null) {
            return cq.getQueueItem(offset, count);
        }
        return new ArrayList<QueueItem>();
    }

    /**
     * 恢复
     *
     * @param recoverFiles 恢复的文件数量
     * @return 待恢复的日志位置
     * @throws IOException
     */
    public long recover(final int recoverFiles) throws IOException {
        long journalOffset = -1;
        for (ConcurrentMap<Integer, ConsumeQueue> consumeQueues : queues.values()) {
            for (ConsumeQueue consumeQueue : consumeQueues.values()) {
                long offset = consumeQueue.recover(recoverFiles);
                if (offset != -1) {
                    if (journalOffset == -1) {
                        journalOffset = offset;
                    } else {
                        journalOffset = Math.min(offset, journalOffset);
                    }
                }
            }
        }
        return journalOffset;
    }

    /**
     * 裁剪越界的队列数据
     *
     * @param journalOffset 日志位置
     * @throws IOException
     */
    public void truncate(long journalOffset) throws IOException {
        for (ConcurrentMap<Integer, ConsumeQueue> consumeQueues : queues.values()) {
            for (ConsumeQueue consumeQueue : consumeQueues.values()) {
                consumeQueue.truncateByJournalOffset(journalOffset);
            }
        }
    }

    /**
     * 获取文件信息
     *
     * @return 文件信息
     */
    public FileStat getFileStat() {
        return appendFileManager.getFileStat();
    }

    /**
     * 返回主题的队列
     *
     * @param topic 主题
     * @return 该主题的消费队列集合
     */
    public ConcurrentMap<Integer, ConsumeQueue> getQueues(String topic) {
        return queues.get(topic);
    }

    /**
     * 返回所有主题的队列
     *
     * @return 所有主题的队列
     */
    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> getQueues() {
        return queues;
    }

}