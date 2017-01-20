package com.ipd.jmq.server.store;

import com.ipd.jmq.common.message.BrokerPrepare;
import com.ipd.jmq.common.message.QueueItem;
import com.ipd.jmq.server.store.journal.AppendFile;
import com.ipd.jmq.server.store.journal.FileStat;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

/**
 * 清理服务
 */
public class CleanupService extends Service {
    private static Logger logger = LoggerFactory.getLogger(CleanupService.class);
    // 存储配置
    private StoreConfig config;
    // 日志管理器
    private JournalManager journalManager;
    // 队列管理器
    private QueueManager queueManager;
    // 检查点
    private CheckPoint checkPoint;
    // 上次清理时间
    private long cleanTime;
    //最小未消费日志的队列信息
    private QueueItem minQueueItem = null;
    private Map<String, Long> minAckOffsets = null;
    //日志打印间隔
    private long logPrintInterval = 1000 * 60 * 5;
    protected final Lock writeLock = rwLock.writeLock();

    public CleanupService(StoreConfig config, JournalManager journalManager,
                          QueueManager queueManager, CheckPoint checkPoint) {
        if (config == null) {
            throw new IllegalArgumentException("config can not be null.");
        }
        if (journalManager == null) {
            throw new IllegalArgumentException("journalManager can not be null.");
        }

        if (queueManager == null) {
            throw new IllegalArgumentException("queueManager can not be null.");
        }
        if (checkPoint == null) {
            throw new IllegalArgumentException("checkPoint can not be null.");
        }
        this.config = config;
        this.journalManager = journalManager;
        this.queueManager = queueManager;
        this.checkPoint = checkPoint;
    }


    @Override
    protected void doStop() {
        super.doStop();
        logger.info("Cleanup service is stopped.");
    }

    /**
     * 清理
     */
    protected void cleanup(Map<String, Long> minAckOffsets) {
        if (!isStarted()) {
            return;
        }
        this.minAckOffsets = minAckOffsets;
        // 尝试加锁，避免已经清理了
        boolean isLocked = writeLock.tryLock();
        if (isLocked) {
            try {
                if (SystemClock.now() - cleanTime > logPrintInterval + config.getCleanInterval() * 2) {
                    cleanTime = SystemClock.now();
                }
                // 清理日志
                cleanupJournal();
                // 清理队列
                cleanupQueue();
                // 判断磁盘空间，如果不够则强制清理
                tryForceCleanup();
                // 清理过期的事务
                cleanupExpiredTransaction();
            } finally {
                writeLock.unlock();
            }
        }
    }

    /**
     * 清理所有主题和队列
     */
    public void cleanupAll() throws IOException {
        writeLock.lock();
        try {
            logger.error("clean all queue and journal");
            cleanupAllQueue();
            cleanupAllJournal();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 当磁盘快满的时候，强制清理
     */
    public void forceCleanup() {
        // 尝试锁
        boolean isLocked = writeLock.tryLock();
        if (isLocked) {
            try {
                tryForceCleanup();
            } finally {
                writeLock.unlock();
            }
        }
    }

    /**
     * 尝试强制清理
     */
    protected void tryForceCleanup() {
        if (config.getFileReserveTime() <= config.getCleanupReserveTime()) {
            return;
        }
        // 获取文件统计
        FileStat fileStat = journalManager.getDiskFileStat();
        // 计算磁盘清理阀值
        long cleanupThreshold = config.getMaxDiskSpace() * config.getCleanupWhenUsage() / 100;
        // 计算数据文件超过了多少空间
        long exceed = fileStat.getAppendFileLength() - cleanupThreshold;
        // 如果文件的保留时间小于了强制清理的文件保留时间，则不用强制清理
        if (exceed > 0 || fileStat.getAppendFiles() > config.getFileLimit()) {
            // 取得日志文件数量
            int size = journalManager.getJournalDir().files().size();
            // 计算需要删除的文件数量
            long deleteFiles = exceed / config.getJournalConfig().getDataSize();
            if (deleteFiles < 2) {
                // 最少删除2个文件
                deleteFiles = 2;
            }
            // 不能删除日志文件的1/4
            deleteFiles = Math.min(size / 4, (int) deleteFiles);
            logger.warn("force clean journal files " + deleteFiles);
            cleanupJournal(config.getCleanupReserveTime(), 0, (int) deleteFiles);
            logger.warn("force clean queue files");
            // 索引文件不方便统计文件数，没有去计算索引的文件
            cleanupQueue(config.getCleanupReserveTime(), 0);
        }
    }

    /**
     * 清除日志
     */
    protected void cleanupJournal() {
        cleanupJournal(config.getFileReserveTime(), config.getDeleteFilesInterval(), 0);
    }

    protected void cleanupExpiredTransaction() {
        List<BrokerPrepare> prepares = journalManager.cleanExpiredTransaction();
        if (prepares.size() > 0) {
            logger.warn(String.format("Clean expired transaction. [%s]", prepares));
        }
    }


    /**
     * 清除日志
     *
     * @param reserveTime         文件保留时间
     * @param deleteFilesInterval 删除文件间隔
     * @param maxDeletes          删除文件最大数量
     */
    protected void cleanupJournal(final long reserveTime, final int deleteFilesInterval, final int maxDeletes) {
        try {
            long journalOffset = getMinAckJournalOffset();
            if (logger.isTraceEnabled()) {
                logger.trace(String.format("minAckJournal:" + journalOffset));
            }
            try {
                if (journalOffset > 0) {
                    AppendFile file = journalManager.getJournalDir().get(journalOffset);
                    if (file != null && (SystemClock.now() - file
                            .header().getCreateTime()) > reserveTime && (SystemClock.now() - file
                            .header().getCreateTime()) > 3 * 24 * 3600 * 1000) {
                        //打印不能清理日志的最小队列信息，方便查找原因
                        logger.warn("min queue item:" + minQueueItem.toString());
                    } else if (SystemClock.now() - cleanTime > logPrintInterval) {
                        logger.info("min queue item:" + minQueueItem.toString());
                    }
                }
            } catch (Throwable ignored) {

            }
            //至少保留一个文件
            AppendFile af = journalManager.getJournalDir().last();
            long lastFileId = 0;
            if (af != null) {
                lastFileId = af.id();
            }
            journalOffset = Math.min(Math.min(checkPoint.getRecoverOffset(), journalOffset), lastFileId);
            if (logger.isInfoEnabled()) {
                if (SystemClock.now() - cleanTime > logPrintInterval) {
                    logger.info(String.format("truncate journal offset:%d,retentionTime:%d", journalOffset,
                            config.getFileReserveTime()));
                }
            }
            journalManager.truncateBefore(journalOffset, reserveTime, deleteFilesInterval, maxDeletes);
        } catch (Exception e) {
            logger.error("cleanup journal error.", e);
        }
    }

    /**
     * 获取最小日志消费位置
     *
     * @return 最小日志消费位置
     * @throws IOException
     */
    public long getMinAckJournalOffset() throws IOException {
        long journalOffset = -1;
        ConsumeQueue queue;
        List<QueueItem> items;
        Long minAckOffset;
        // 获取队列最小应答位置
        Map<ConsumeQueue, Long> minAckOffsets = getMinQueueOffset();
        // 遍历队列
        for (Map.Entry<String, ConcurrentMap<Integer, ConsumeQueue>> e : queueManager.getQueues().entrySet()) {
            for (ConcurrentMap.Entry<Integer, ConsumeQueue> cqEntry : e.getValue().entrySet()) {
                queue = cqEntry.getValue();
                // 获取当前队列的最小应答位置
                minAckOffset = minAckOffsets.get(queue);
                if (minAckOffset == null) {
                    // 并发调整了队列数
                    minAckOffset = -1L;
                }
                if (queue.getMaxOffset() == 0 || minAckOffset >= queue.getMaxOffset()) {
                    // 当前队列没有数据或者数据都已经消费
                    if (journalOffset < 0) {
                        journalOffset = checkPoint.getRecoverOffset();
                        minQueueItem = null;
                    } else {
                        if (journalOffset > checkPoint.getRecoverOffset()) {
                            journalOffset = checkPoint.getRecoverOffset();
                            minQueueItem = null;
                        }
                    }
                } else {
                    if (minAckOffset <= 0) {
                        // 取第一条数据
                        items = queue.getQueueItem(queue.getMinOffset(), 1);
                    } else if (minAckOffset >= ConsumeQueue.CQ_RECORD_SIZE) {
                        // 取最后一条应答位置的数据
                        items = queue.getQueueItem(minAckOffset - ConsumeQueue.CQ_RECORD_SIZE, 1);
                    } else {
                        items = null;
                    }
                    if (items != null && !items.isEmpty()) {
                        QueueItem tmp = items.get(0);
                        if (journalOffset < 0 || journalOffset > tmp.getJournalOffset()) {
                            journalOffset = tmp.getJournalOffset();
                            minQueueItem = new QueueItem();
                            minQueueItem.setTopic(tmp.getTopic());
                            minQueueItem.setQueueId(tmp.getQueueId());
                            minQueueItem.setQueueOffset(tmp.getQueueOffset());
                            minQueueItem.setJournalOffset(tmp.getJournalOffset());
                        }
                    }
                }
                if (logger.isDebugEnabled()) {
                    if (logger.isTraceEnabled() || (SystemClock.now() - cleanTime > logPrintInterval)) {
                        logger.debug(String.format("topic:%s,queue:%d,consume min offset:%d,journalOffset:%d",
                                queue.getTopic(), queue.getQueueId(), minAckOffset, journalOffset));
                    }
                }
            }
        }
        if (journalOffset < 0) {
            journalOffset = 0;
        }

        return journalOffset;
    }

    /**
     * 清理消费者队列
     *
     * @param reserveTime         保留的时间
     * @param deleteFilesInterval 删除文件时间间隔
     */
    protected void cleanupQueue(final long reserveTime, final int deleteFilesInterval) {
        Map<ConsumeQueue, Long> offsets = getMinQueueOffset();
        Long offset;
        ConsumeQueue queue;
        AppendFile af;
        long lastFileId;
        //进行裁剪
        for (Map.Entry<ConsumeQueue, Long> entry : offsets.entrySet()) {
            if (!isStarted()) {
                return;
            }
            offset = entry.getValue();
            queue = entry.getKey();
            if (offset == null || offset <= 0) {
                continue;
            }
            try {
                //至少保留最后一个文件
                lastFileId = 0;
                af = queue.getAppendDirectory().last();
                if (af != null) {
                    lastFileId = af.id();
                }
                queue.truncateBefore(journalManager.getMinOffset(), Math.min(lastFileId, offset), reserveTime,
                        deleteFilesInterval);
            } catch (IOException e) {
                logger.error("clean up queue error.", e);
            }
        }

    }


    /**
     * 清理消费者队列
     */
    protected void cleanupQueue() {
        cleanupQueue(config.getFileReserveTime(), config.getDeleteFilesInterval());
    }


    /**
     * 清理队列数据
     *
     * @throws IOException
     */
    protected void cleanupAllQueue() throws IOException {
        ConcurrentMap<Integer, ConsumeQueue> queues;
        ConsumeQueue cq;
        for (Map.Entry<String, ConcurrentMap<Integer, ConsumeQueue>> entry : queueManager.getQueues().entrySet()) {
            queues = entry.getValue();
            for (ConcurrentMap.Entry<Integer, ConsumeQueue> cqe : queues.entrySet()) {
                if (!isStarted()) {
                    return;
                }
                cq = cqe.getValue();
                cq.truncateAll(0);
            }
        }
    }

    /**
     * 清除日志
     */
    protected void cleanupAllJournal() throws IOException {
        journalManager.truncateAll(0);
    }

    /**
     * 获取队列最小的应答位置，确保能安全清理
     *
     * @return 队列的最小位置
     */
    protected Map<ConsumeQueue, Long> getMinQueueOffset() {
        Map<String, Long> minAckOffsets = this.minAckOffsets;
        Map<ConsumeQueue, Long> offsets = new HashMap<ConsumeQueue, Long>();
        if (minAckOffsets == null){
            return offsets;
        }
        String topic;
        ConcurrentMap<Integer, ConsumeQueue> queues;
        ConsumeQueue queue;
        Integer queueId;
        Long offset;
        // 遍历主题的队列
        for (Map.Entry<String, ConcurrentMap<Integer, ConsumeQueue>> entry : queueManager.getQueues().entrySet()) {
            // 主题
            topic = entry.getKey();
            // 队列
            queues = entry.getValue();


            // 遍历队列
            for (ConcurrentMap.Entry<Integer, ConsumeQueue> cq : queues.entrySet()) {
                // 队列
                queueId = cq.getKey();
                queue = cq.getValue();
                // 判断是否有订阅者
                String key = queue.getTopic() +":" + queue.getQueueId();
                offset = minAckOffsets.get(key);
                if (offset != null && offset >= 0){
                    offsets.put(queue, offset);
                }
                if (logger.isDebugEnabled()) {
                    if (SystemClock.now() - cleanTime > logPrintInterval) {
                        logger.debug(String.format("topic:%s,queue:%d,consume min offset:%d", topic, queueId, offset));
                    }
                }
            }
        }
        return offsets;
    }


}