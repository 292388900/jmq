package com.ipd.jmq.server.broker.dispatch;

import com.ipd.jmq.common.message.MessageLocation;
import com.ipd.jmq.common.network.v3.session.Consumer;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.SessionManager;
import com.ipd.jmq.server.broker.offset.OffsetBlock;
import com.ipd.jmq.server.broker.offset.OffsetManager;
import com.ipd.jmq.server.broker.offset.UnSequenceOffset;
import com.ipd.jmq.server.broker.sequence.Sequence;
import com.ipd.jmq.server.broker.sequence.SequenceSet;
import com.ipd.jmq.server.store.ConsumeQueue;
import com.ipd.jmq.server.store.Store;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.service.ServiceThread;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by dingjun on 2016/5/13.
 */
public class ConcurrentPull extends Service {
    private static Logger logger = LoggerFactory.getLogger(ConcurrentPull.class);
    ConcurrentHashMap<PullStatKey, PullStat> pullStats = new ConcurrentHashMap<PullStatKey, PullStat>();
    private DispatchManager dispatchManager;
    private OffsetManager offsetManager;
    private BrokerConfig brokerConfig;

    public ConcurrentPull(){}
    public ConcurrentPull(DispatchManager dispatchManager, BrokerConfig brokerConfig, OffsetManager offsetManager) {
        this.dispatchManager = dispatchManager;
        this.brokerConfig = brokerConfig;
        this.offsetManager = offsetManager;
    }

    private Thread expireCleaner;

    public void setDispatchManager(DispatchManager dispatchManager) {
        this.dispatchManager = dispatchManager;
    }

    public void setOffsetManager(OffsetManager offsetManager) {
        this.offsetManager = offsetManager;
    }

    public void setBrokerConfig(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
    }

    @Override
    public void doStart() {
        //过期未应答数据清理
        expireCleaner = new Thread(new ServiceThread(this) {
            @Override
            protected void execute() throws Exception {
                cleanExpire(dispatchManager);
            }

            @Override
            public long getInterval() {
                return brokerConfig.getCheckBlockExpireInterval();
            }

            @Override
            public boolean onException(Throwable e) {
                logger.error(e.getMessage(), e);
                return true;
            }
        }, "JMQ_SERVER_CONCURRENT_PULL_EXPIRE_CLEANER");
        expireCleaner.setDaemon(true);
        expireCleaner.start();
        logger.info("Concurrent pull expire cleaner started.");

    }

    @Override
    public void doStop() {
        try {
            if (expireCleaner != null) {
                expireCleaner.interrupt();
            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    public PullStat getOrCreatePullStat(String topic, String app, int queueId) {
        PullStatKey key = new PullStatKey(topic, app, queueId);
        PullStat curPullStat = pullStats.get(key);
        if (curPullStat != null) {
            return curPullStat;
        } else {
            PullStat pullStat = new PullStat(topic, app, queueId);
            PullStat old = pullStats.putIfAbsent(key, pullStat);
            if (old != null) {
                pullStat = old;
            }
            return pullStat;
        }
    }

    public OffsetBlock getConsumeMessageBlock(String consumerId, PullStat pullStat, Store store, long timeout, int
            count, int maxLockSize) throws Exception {
        long maxOffset = store.getMaxOffset(pullStat.getTopic(), pullStat.queueId);
        OffsetBlock block = null;
        boolean isLock = pullStat.lock.writeLock().tryLock(10, TimeUnit.MILLISECONDS);
        if (!isLock) {
            if (logger.isDebugEnabled()) {
                logger.debug("lock failure:" + consumerId + ",topic:" + pullStat.getTopic() + ",qid:" + pullStat.getQueueId() + ",app:" + pullStat.getApp());
            }
            return null;
        }
        try {

            //先推送过期的数据
            block = pullStat.getFirstExpiredMessageBlockNoLock();
            boolean isExpired = false;
            if (block == null) {

                if (pullStat.lockMoreNoLock(consumerId, maxLockSize)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("lock more:" + consumerId + ",topic:" + pullStat.getTopic() + ",qid:" + pullStat.getQueueId() + ",app:" + pullStat.getApp() + ",max:" + maxLockSize);
                    }
                    return block;
                }

                UnSequenceOffset offset = dispatchManager.getUnSequenceOffset(pullStat.getTopic(), pullStat.getApp(), pullStat
                        .getQueueId());
                if (offset == null) {
                    //高优先级可能没有创建队列
                    return null;
                }
                block = pullStat.getPullEndNoLock(offset, count);
                if (block.getMinQueueOffset() >= maxOffset) {
                    //没有消息可以拉取
                    return null;
                } else if (block.getMaxQueueOffset() > maxOffset - ConsumeQueue.CQ_RECORD_SIZE) {
                    block = new OffsetBlock(null, block.getMinQueueOffset(), maxOffset - ConsumeQueue.CQ_RECORD_SIZE);
                }
            } else {
                //过期的BLOCK要重置是否被派发的标识
                block.setDispatched(false);
                isExpired = true;
            }
            block.setConsumerId(consumerId);
            block.setQueueId(pullStat.getQueueId());
            block.setExpireTime(SystemClock.now() + timeout);
            //放入未确认列表，如果读取消息出错可以设置超时
            pullStat.addNoAckBlockNoLock(block);
            if (logger.isDebugEnabled()) {
                logger.debug("Dispatch:" + block);
            }
            if (isExpired) {
                pullStat.removeExpiredMessageBlockNoLock(block);
            }
        } finally {
            pullStat.lock.writeLock().unlock();
        }
        return block;
    }

    public short getMessageCount(OffsetBlock block) {
        long start = block.getMinQueueOffset();
        long end = block.getMaxQueueOffset();
        int getCount;
        //计算消息条数
        if (start == end) {
            getCount = 1;
        } else {
            getCount = (int) ((end - start) / ConsumeQueue.CQ_RECORD_SIZE + 1);
        }
        if (getCount > Short.MAX_VALUE) {
            throw new RuntimeException(String.format("Invalid range[%d - %d]", start, end));
        }
        return (short) getCount;
    }

    public void cleanExpire(DispatchManager dispatchManager) {
        try {
            int i = 0;
            for (PullStat pullStat : pullStats.values()) {
                List<OffsetBlock> list = pullStat.checkExpired();
                for (OffsetBlock block : list) {
                    dispatchManager.addConsumeErr(block.getConsumerId(), true);
                    logger.error("Transfer expire block" + block);
                }
                i++;
                if (i % 20 == 0) {
                    Thread.sleep(2);
                }
            }

        } catch (Throwable e) {
            logger.error("Clean expire error.", e);
        }
    }

    public void addSessionListener(SessionManager sessionManager) {
        sessionManager.addListener(new EventListener<SessionManager.SessionEvent>() {
            @Override
            public void onEvent(SessionManager.SessionEvent sessionEvent) {

                if (sessionEvent.getType() == SessionManager.SessionEventType.RemoveConsumer) {
                    Consumer consumer = sessionEvent.getConsumer();
                    onRemoveConsumer(consumer.getTopic(), consumer.getApp(), consumer.getId());
                }
            }
        });
    }

    public void onRemoveConsumer(String topic, String app, String consumerId) {
        //移除consumer时，将对应的noAckBlock全部过期，以尽快推送给其他的consumer
        List<Integer> queues =  brokerConfig.getStore().getQueues(topic);
        if (queues != null) {
            for (Integer queueId : queues) {
                PullStat pullStat = pullStats.get(new PullStatKey(app, topic, queueId));
                if (pullStat != null) {
                    pullStat.expireBlocks(consumerId);
                }
            }
        }
    }

    public boolean ack(String topic, String app, String consumerId, final List<MessageLocation> locationList, Store store) {
        MessageLocation first = locationList.get(0);
        MessageLocation last = locationList.get(locationList.size() - 1);

        //检查Ack是否连续
        int messageSize = (int) ((last.getQueueOffset() - first.getQueueOffset()) / ConsumeQueue.CQ_RECORD_SIZE + 1);
        if (first.getQueueId() != last.getQueueId() || messageSize != locationList.size()) {
            throw new IllegalArgumentException("Not consecutive ack ackSize:" + messageSize + "listSize" + locationList.size() + ";" + topic + ";" + app + ";" + consumerId);
        }
        //收到ack,consumer可以正常消费了
        dispatchManager.cleanConsumeErr(consumerId);
        OffsetBlock block = new OffsetBlock(consumerId, first.getQueueId(), first.getQueueOffset(), last.getQueueOffset());
        PullStat pullStat = getOrCreatePullStat(topic, app, first.getQueueId());
        boolean flag = false;
        flag = pullStat.isNoAck(block);
        if (!flag) {
            logger.error("Block is not in noAck:" + block);
        } else {
            pullStat.lock.writeLock().lock();
            try {
                for (MessageLocation ackLoc : locationList) {
                    dispatchManager.acknowledge(new MessageLocation(topic, (short)block.getQueueId(), ackLoc
                            .getQueueOffset()), app);
                }
                flag = pullStat.ackNoLock(block);
                if (flag && logger.isDebugEnabled()) {
                    logger.debug("Ack:" + block);
                }
            } finally {
                pullStat.lock.writeLock().unlock();
            }
        }
        return flag;
    }

    public Map<PullStatKey, List<OffsetBlock>> getNoAckBlocks() {
        Map<PullStatKey, List<OffsetBlock>> noAckBlocks = new HashMap<PullStatKey, List<OffsetBlock>>();
        for (Map.Entry<PullStatKey, PullStat> tmp : pullStats.entrySet()) {
            List<OffsetBlock> blocks = noAckBlocks.get(tmp.getKey());
            if (blocks == null) {
                blocks = new ArrayList<OffsetBlock>();
            }
            blocks.addAll(tmp.getValue().noAckBlocks.keySet());
            if (blocks.size() > 0) {
                noAckBlocks.put(tmp.getKey(), blocks);
            }
        }
        return noAckBlocks;
    }


    public class PullStat {
        private String app;
        private String topic;
        private int queueId;
        private ConcurrentMap<OffsetBlock, OffsetBlock> noAckBlocks = new ConcurrentHashMap<OffsetBlock, OffsetBlock>();
        private ConcurrentMap<OffsetBlock, OffsetBlock> noAckExpiredBlocks = new ConcurrentHashMap<OffsetBlock, OffsetBlock>();
        private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private AtomicLong pullEndOffset = new AtomicLong(0 - ConsumeQueue.CQ_RECORD_SIZE);

        public PullStat(String topic, String app, int queueId) {
            this.topic = topic;
            this.app = app;
            this.queueId = queueId;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getQueueId() {
            return queueId;
        }

        public void setQueueId(short queueId) {
            this.queueId = queueId;
        }

        public List<OffsetBlock> checkExpired() throws CloneNotSupportedException {

            List<OffsetBlock> expired = new ArrayList<OffsetBlock>();
            lock.writeLock().lock();
            try {
                Iterator<Map.Entry<OffsetBlock, OffsetBlock>> it = noAckBlocks.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<OffsetBlock, OffsetBlock> entry = it.next();
                    if (entry.getKey().expired()) {
                        //转移到超时队列
                        Object obj = noAckExpiredBlocks.putIfAbsent(entry.getKey(), entry.getKey());
                        if (obj == null && entry.getKey().isDispatched()) {
                            //不在过期列表里，说明是第一次过期或者已经被取出过
                            expired.add(entry.getKey().copy());
                        }
                    }
                }
            } finally {
                lock.writeLock().unlock();
            }

            return expired;
        }

        public void expireBlocks(String consumerId) {
            lock.writeLock().lock();
            try {
                Set<OffsetBlock> allBlocks = noAckBlocks.keySet();
                for (OffsetBlock block : allBlocks) {
                    if (block.getConsumerId() != null && block.getConsumerId().equals(consumerId)) {
                        noAckExpiredBlocks.putIfAbsent(block, block);
                    }
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        public void splitMessageBlock(OffsetBlock old, OffsetBlock last, List<OffsetBlock> partitions) {
            lock.writeLock().lock();
            try {
                noAckExpiredBlocks.remove(old);
                noAckBlocks.put(last, last);
                for (OffsetBlock cur : partitions) {
                    noAckBlocks.put(cur, cur);
                    noAckExpiredBlocks.put(cur, cur);
                }
                noAckBlocks.remove(old);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public void releaseBlock(OffsetBlock old, long expireTime) {
            lock.writeLock().lock();
            try {
                if (expireTime < old.getExpireTime()) {
                    //更新一下过期时间，加速过期
                    old.setExpireTime(expireTime);
                    noAckBlocks.put(old, old);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        private OffsetBlock removeExpiredMessageBlockNoLock(OffsetBlock block) {
            return noAckExpiredBlocks.remove(block);
        }

        private OffsetBlock getFirstExpiredMessageBlockNoLock() {

            if (noAckExpiredBlocks.isEmpty()) {
                return null;
            }
            Iterator<Map.Entry<OffsetBlock, OffsetBlock>> it = noAckExpiredBlocks.entrySet().iterator();
            if (it.hasNext()) {
                return it.next().getKey();
            }

            return null;
        }

        private boolean lockMoreNoLock(String consumerId, int maxSize) {
            int num = 0;
            int count = 0;
            for (Map.Entry<OffsetBlock, OffsetBlock> entry : noAckBlocks.entrySet()) {
                if (entry.getKey().getConsumerId().equals(consumerId)) {
                    num += entry.getKey().size();
                }
                count += entry.getKey().size();
            }
            if (count >= maxSize || num >= maxSize / 2) {
                return true;
            } else {
                return false;
            }
        }

        private void addNoAckBlockNoLock(OffsetBlock block) {
            Object v = noAckBlocks.putIfAbsent(block, block);

            if (pullEndOffset.get() < block.getMaxQueueOffset()) {
                pullEndOffset.set(block.getMaxQueueOffset());
            }

        }

        private OffsetBlock getPullEndNoLock(UnSequenceOffset offset, int maxCount) {

            OffsetBlock block = null;
            long nextPullOffset = -1;

            if (pullEndOffset.get() >= 0) {
                nextPullOffset = pullEndOffset.get() + ConsumeQueue.CQ_RECORD_SIZE;
            } else {
                nextPullOffset = offset.getAckOffset().get();
            }
            SequenceSet acks = offset.getAcks();
            List<Sequence> sequenceList = acks.toArrayList();
            for (int i = 0; i < sequenceList.size(); i++) {
                Sequence sequence = sequenceList.get(i);
                long sequenceFirst = sequence.getFirst();
                if (nextPullOffset + ConsumeQueue.CQ_RECORD_SIZE == sequenceFirst) {
                    //说明已经确认过了，就对齐到该块的最后位置
                    nextPullOffset = sequence.getLast();
                } else if (sequenceFirst > nextPullOffset) {
                    //最后确认的位置已经是确认过索引的最后位置，需要推算到前一条索引的开始的位置
                    short noAckCount = (short) (messageCount(nextPullOffset, sequenceFirst - ConsumeQueue.CQ_RECORD_SIZE - ConsumeQueue.CQ_RECORD_SIZE));
                    if (noAckCount > maxCount) {
                        //acks中的非连续noAck条数大于count，只拉取count条
                        block = new OffsetBlock(nextPullOffset, maxCount);
                    } else {
                        //acks中的非连续noAck条数<count,则推送noAckCount条
                        block = new OffsetBlock(null, nextPullOffset, sequenceFirst - ConsumeQueue.CQ_RECORD_SIZE - ConsumeQueue.CQ_RECORD_SIZE);
                    }
                    break;
                } else if (sequence.getLast() > nextPullOffset) {
                    nextPullOffset = sequence.getLast();
                }
            }

            if (block == null) {
                block = new OffsetBlock(nextPullOffset, maxCount);
            }
            return block;
        }


        private int messageCount(long start, long end) {
            if (start > end) {
                return 0;
            }

            if (start == end) {
                return 1;
            }

            return (int) ((end - start) / ConsumeQueue.CQ_RECORD_SIZE + 1);
        }

        private int noAckBlockSizeNoLock() {
            return noAckBlocks.size();
        }

        private boolean ackNoLock(OffsetBlock offsetBlock) {
            noAckExpiredBlocks.remove(offsetBlock);
            Object v = noAckBlocks.remove(offsetBlock);
            return v != null;

        }

        public boolean ack(OffsetBlock offsetBlock) {
            lock.writeLock().lock();
            try {
                return ackNoLock(offsetBlock);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public boolean isNoAck(OffsetBlock block) {
            return noAckBlocks.containsKey(block);
        }

        public String getApp() {
            return app;
        }

        public void setApp(String app) {
            this.app = app;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PullStat)) return false;

            PullStat pullStat = (PullStat) o;

            if (queueId != pullStat.queueId) return false;
            if (!app.equals(pullStat.app)) return false;
            if (!topic.equals(pullStat.topic)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = app.hashCode();
            result = 31 * result + topic.hashCode();
            result = 31 * result + (int) queueId;
            return result;
        }

        public long getPullEndOffset() {
            return pullEndOffset.get();
        }
    }


    public class PullStatKey {
        private String app;
        private String topic;
        private int queueId;

        public PullStatKey(String app, String topic, int queueId) {
            this.app = app;
            this.topic = topic;
            this.queueId = queueId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PullStatKey)) return false;

            PullStatKey that = (PullStatKey) o;

            if (queueId != that.queueId) return false;
            if (!app.equals(that.app)) return false;
            if (!topic.equals(that.topic)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = app.hashCode();
            result = 31 * result + topic.hashCode();
            result = 31 * result + (int) queueId;
            return result;
        }
    }

}
