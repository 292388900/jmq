package com.ipd.jmq.server.store;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.cluster.BrokerGroup;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.*;
import com.ipd.jmq.common.model.JournalLog;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
import com.ipd.jmq.server.store.journal.FileStat;
import com.ipd.jmq.toolkit.buffer.RByteBuffer;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.service.ServiceThread;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.*;
import java.util.concurrent.*;

/**
 * 存储服务.
 *
 * @author lindeqiang
 * @since 2016/7/26 15:39
 */
public class StoreService extends Service implements Store, StoreUnsafe {
    private static Logger logger = LoggerFactory.getLogger(StoreService.class);
    // 当前broker
    protected Broker broker;
    // JMQ存储实现
    protected JMQStore store;
    // 存储配置
    protected StoreConfig storeConfig;
    // Broker 集群管理器
    protected ClusterManager clusterManager;
    // broker分组
    protected BrokerGroup group = new BrokerGroup();

    protected com.ipd.jmq.server.broker.dispatch.DispatchService dispatchService;

    // todo: 考虑slave不可用时会导致大量pendingMessages
    protected List<PendingMessage> pendingMessages = new ArrayList<PendingMessage>();
    protected Thread pendingMessageCheckThreadInstance;
    protected PendingMessageCheckThread pendingMessageCheckThread;

    // 已经复制并刷盘的JournalOffset
    private volatile long waterMark = 0;

    public StoreService(StoreConfig storeConfig) {
        this.storeConfig = storeConfig;
    }

    public StoreService(StoreConfig storeConfig, Broker broker, ClusterManager clusterManager) {
        this.storeConfig = storeConfig;
        this.broker = broker;
        this.clusterManager = clusterManager;
        this.pendingMessageCheckThread = new PendingMessageCheckThread(this);
        this.pendingMessageCheckThreadInstance = new Thread(pendingMessageCheckThread);
        this.pendingMessageCheckThreadInstance.setDaemon(true);
    }

    @Override
    protected void validate() throws Exception {
        super.validate();
        Preconditions.checkArgument(clusterManager != null, "clusterManager can not be null");
        Preconditions.checkArgument(storeConfig != null, "storeConfig can not be null");
        Preconditions.checkArgument(dispatchService != null, "dispatch service can not be null");
    }

    public void doStart() throws Exception {
        super.doStart();
        store.start();
        pendingMessageCheckThreadInstance.start();
    }

    public void doStop() {
        super.stop();
    }

    public void setDispatchService(DispatchService dispatchService) {
        this.dispatchService = dispatchService;
    }

    @Override
    public long getMaxOffset(String topic, int queueId) {
        return store.getMaxOffset(topic, queueId);
    }

    @Override
    public long getMinOffset(String topic, int queueId) {
        return store.getMinOffset(topic, queueId);
    }

    @Override
    public long getMaxOffset() {
        return store.getMaxOffset();
    }

    @Override
    public long getMinOffset() {
        return store.getMinOffset();
    }

    @Override
    public long getNextOffset(String topic, int queueId, long offset) {
        return store.getNextOffset(topic, queueId, offset);
    }

    @Override
    public RByteBuffer getMessage(String topic, int queueId, long queueOffset) throws JMQException {
        return store.getMessage(topic, queueId, queueOffset);
    }

    @Override
    public QueueItem getQueueItem(String topic, int queueId, long queueOffset) throws JMQException {
        return store.getQueueItem(topic, queueId, queueOffset);
    }

    @Override
    public QueueItem getMinQueueItem(String topic, int queueId, long minOffset, long maxOffset) throws JMQException {
        return store.getMinQueueItem(topic, queueId, minOffset, maxOffset);
    }

    @Override
    public GetResult getMessage(String topic, int queueId, long queueOffset, int count, long maxFrameSize) throws
            JMQException {
        return store.getMessage(topic, queueId, queueOffset, count, maxFrameSize);
    }

    @Override
    public GetResult getMessage(String topic, int queueId, long queueOffset, int count, long maxFrameSize, int delay) throws JMQException {
        return store.getMessage(topic, queueId, queueOffset, count, maxFrameSize, delay);
    }

    @Override
    public ConcurrentMap<String, List<Integer>> getQueues() {
        return store.getQueues();
    }

    @Override
    public List<Integer> getQueues(String topic) {
        return store.getQueues(topic);
    }


    public List<Integer> updateQueues(String topic, int queues) throws JMQException {
        return store.updateQueues(topic, queues);
    }

    @Override
    public void addQueue(String topic, short queueId) throws JMQException {
        store.addQueue(topic, queueId);
    }

    @Override
    public boolean isDiskFull() {
        return store.isDiskFull();
    }

    @Override
    public FileStat getDiskFileStat() {
        return store.getDiskFileStat();
    }

    @Override
    public StoreConfig getConfig() {
        return store.getConfig();
    }

    @Override
    public boolean hasBadBlock() {
        return store.hasBadBlock();
    }

    @Override
    public long getMinOffsetTimestamp(String topic, int queueId) throws JMQException {
        return store.getMinOffsetTimestamp(topic, queueId);
    }

    @Override
    public long getMaxOffsetTimestamp(String topic, int queueId) throws JMQException {
        return store.getMaxOffsetTimestamp(topic, queueId);
    }

    @Override
    public Set<Long> getQueueOffsetsBefore(String topic, int queueId, long timestamp, int maxCount) throws JMQException {
        return store.getQueueOffsetsBefore(topic, queueId, timestamp, maxCount);
    }

    @Override
    public long getQueueOffsetByTimestamp(String topic, int queueId, String consumer, long resetAckTimeStamp, long
            nextAckOffset) throws JMQException {
        return store.getQueueOffsetByTimestamp(topic, queueId, consumer, resetAckTimeStamp, nextAckOffset);
    }

    @Override
    public PutResult putMessages(final List<BrokerMessage> messages, final ProcessedCallback callback) throws JMQException {
        PutResult result = putJournalLogs(messages);
        if (result != null) {
            result.setWaterMark(waterMark);
        }
        if (callback != null && result != null && result.getLocation() != null && result.getLocation().size() > 0) {
            long journalOffset = messages.get(messages.size() - 1).getJournalOffset();
            if (journalOffset <= waterMark) {
                result.setCode(JMQCode.SUCCESS);
                result.setWaterMark(waterMark);
                callback.onReplicated(result);
            } else {
                synchronized (pendingMessages) {
                    pendingMessages.add(new PendingMessage(journalOffset, result, callback));
                }
                pendingMessageCheckThread.addTimeout(journalOffset, storeConfig.getFlushTimeout());
            }
        }
        return result;
    }

    @Override
    public PutResult beginTransaction(BrokerPrepare prepare) throws JMQException {
        throw new IllegalStateException("method has not implemented");
    }

    @Override
    public PutResult putTransactionMessages(List<BrokerMessage> preMessages) throws JMQException {
        throw new IllegalStateException("method has not implemented");
    }

    @Override
    public PutResult commitTransaction(BrokerCommit commit) throws JMQException {
        throw new IllegalStateException("method has not implemented");
    }

    @Override
    public PutResult rollbackTransaction(BrokerRollback rollback) throws JMQException {
        throw new IllegalStateException("method has not implemented");
    }

    @Override
    public BrokerPrepare getBrokerPrepare(String txId) {
        return store.getBrokerPrepare(txId);
    }

    @Override
    public void unLockInflightTransaction(String txId) {
        store.unLockInflightTransaction(txId);
    }

    @Override
    public void onTransactionFeedback(String txId) {
        store.onTransactionFeedback(txId);
    }

    @Override
    public int getRestartSuccess() {
        return store.getRestartSuccess();
    }

    @Override
    public int getRestartTimes() {
        return store.getRestartTimes();
    }

    @Override
    public GetTxResult getInflightTransaction(String topic, String owner) throws JMQException {
        return store.getInflightTransaction(topic, owner);
    }

    @Override
    public <T extends JournalLog> PutResult putJournalLogs(List<T> logs) throws JMQException {
        if (logs == null || logs.isEmpty()) {
            return null;
        }
        try {
            T log = logs.get(0);
            PutResult result;

            if (log instanceof BrokerPrepare) {
                result = store.beginTransaction((BrokerPrepare) log);
            } else if (log instanceof BrokerCommit) {
                result = store.commitTransaction((BrokerCommit) log);
            } else if (log instanceof BrokerRollback) {
                result = store.rollbackTransaction((BrokerRollback) log);
            } else if (log instanceof BrokerMessage) {
                short type = -1;
                List<BrokerMessage> messages = new ArrayList<>();
                try {
                    for (JournalLog message : logs) {
                        type = message.getType();
                        BrokerMessage bmMsg = (BrokerMessage) message;
                        dispatchService.dispatchQueue(bmMsg);
                        messages.add(bmMsg);
                    }
                } catch (Exception e) {
                    throw new JMQException(JMQCode.SE_IO_ERROR, e);
                }
                if (type == JournalLog.TYPE_TX_PRE_MESSAGE) {
                    result = store.putTransactionMessages(messages);
                } else {
                    result = store.putMessages(messages, null);
                }

            } else {
                throw new JMQException(JMQCode.SE_INVALID_JOURNAL);
            }

            return result;
        } catch (Exception e) {
            if (e instanceof JMQException) {
                throw (JMQException) e;

            } else {
                throw new JMQException("", e, JMQCode.CN_UNKNOWN_ERROR.getCode());
            }
        }
    }

    @Override
    public <T extends JournalLog> PutResult putJournalLog(T log) throws JMQException {
        if (log == null) {
            return null;
        }

        List<T> logs = new ArrayList<>();
        logs.add(log);
        return putJournalLogs(logs);
    }

    @Override
    public RByteBuffer readMessageBlock(long offset, int size) throws Exception {
        return store.readMessageBlock(offset, size);
    }

    @Override
    public void writeMessageBlock(long offset, RByteBuffer buffer, boolean buildIndex) throws JMQException, Exception {
        store.writeMessageBlock(offset, buffer, buildIndex);
    }

    @Override
    public void cleanUp(Map<String, Long> minAckOffsets) throws Exception {
        store.cleanUp(minAckOffsets);
    }

    public void setStore(JMQStore store) {
        this.store = store;
    }

    @Override
    public void updateWaterMark(long waterMark) {
        this.waterMark = waterMark;
    }

    @Override
    public void truncate(long offset) throws IOException {
        store.truncate(offset);
    }

    protected static class PendingMessage implements Comparable<Long> {
        public PendingMessage(Long journalOffset, PutResult result, ProcessedCallback event) {
            this.journalOffset = journalOffset;
            this.result = result;
            this.event = event;
        }

        @Override
        public int compareTo(Long offset) {
            return journalOffset.compareTo(offset);
        }

        public void done(JMQCode code, long waterMark) {
            result.setReplicationCode(code);
            result.setWaterMark(waterMark);
            event.onReplicated(result);
        }

        public Long journalOffset;
        private PutResult result;
        private ProcessedCallback event;
    }

    protected class PendingMessageCheckThread extends ServiceThread {
        public PendingMessageCheckThread(Service parent) {
            super(parent, 1);
        }

        public void addTimeout(long journalOffset, int timeOut) {
            TimeOutPair timeOutPair = timeOutPairDeque.peekLast();
            long absoluteTimeout = SystemClock.now() + timeOut;
            if (timeOutPair != null && absoluteTimeout - timeOutPair.timeOut < CHECK_TIMEOUT_INTERVAL) {
                synchronized (timeOutPair) {
                    if (timeOutPair.journalOffset < journalOffset) timeOutPair.journalOffset = journalOffset;
                }
            } else {
                timeOutPairDeque.add(new TimeOutPair(absoluteTimeout, journalOffset));
            }
        }

        protected void execute() throws Exception {
            if (pendingMessages.isEmpty()) return;

            boolean isTimeout = false;
            long waterMark = StoreService.this.waterMark;

            if (waterMark < pendingMessages.get(0).journalOffset) {
                // 如果没有待处理的pendingMessages则检查是否有超时的消息
                waterMark = 0;
                isTimeout = true;
                while (!timeOutPairDeque.isEmpty()) {
                    TimeOutPair timeOutPair = timeOutPairDeque.peekFirst();
                    if (timeOutPair != null && timeOutPair.timeOut < SystemClock.getInstance().now()) {
                        timeOutPairDeque.removeFirst();
                        if (waterMark < timeOutPair.journalOffset)
                            waterMark = timeOutPair.journalOffset;
                    } else {
                        break;
                    }
                }
                if (waterMark < pendingMessages.get(0).journalOffset)
                    return;
            }

            Object[] replicatedMessages = null;
            synchronized (pendingMessages) {
                int index = Collections.binarySearch(pendingMessages, waterMark);
                index = ((index < 0) ? -index - 1 : index + 1);
                if (index > 0) {
                    List ackMessages = pendingMessages.subList(0, Math.min(index, pendingMessages.size()));
                    replicatedMessages = ackMessages.toArray();
                    ackMessages.clear();
                }
            }
            if (replicatedMessages != null) {
                if (isTimeout && logger.isDebugEnabled()) {
                    logger.debug("pendingMessages timeout: {}, {}", replicatedMessages.length, ((PendingMessage) replicatedMessages[replicatedMessages.length - 1]).journalOffset);
                }
                for (Object pendingMessage : replicatedMessages) {
                    ((PendingMessage) pendingMessage).done(isTimeout ? JMQCode.SE_FLUSH_TIMEOUT : JMQCode.SUCCESS, waterMark);
                }
            }
        }

        private class TimeOutPair {
            public TimeOutPair(long timeOut, long journalOffset) {
                this.timeOut = timeOut;
                this.journalOffset = journalOffset;
            }

            public long timeOut;
            public long journalOffset;
        }

        private Deque<TimeOutPair> timeOutPairDeque = new ConcurrentLinkedDeque<>();
        private final static int CHECK_TIMEOUT_INTERVAL = 100;
    }
}
