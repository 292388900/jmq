package com.ipd.jmq.server.broker;

import com.ipd.jmq.server.broker.offset.OffsetManager;
import com.ipd.jmq.server.broker.offset.QueueOffset;
import com.ipd.jmq.server.broker.offset.TopicOffset;
import com.ipd.jmq.server.store.*;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.service.ServiceThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * 清理服务
 */
public class CleanupManager extends Service {
    private static Logger logger = LoggerFactory.getLogger(CleanupManager.class);
    // 日志管理器
    private OffsetManager offsetManager;
    // 定时器
    private Thread cleanupThread;
    // 存储服务
    private Store store;

    public CleanupManager(Store store, OffsetManager offsetManager) {
        Preconditions.checkState(store!=null, "store can not be null.");
        Preconditions.checkState(offsetManager!=null, "offsetManager can not be null.");

        this.store = store;
        this.offsetManager = offsetManager;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        cleanupThread = new Thread(new ServiceThread(this) {
            @Override
            protected void execute() throws Exception {
                cleanup();
            }

            @Override
            public long getInterval() {
                return store.getConfig().getCleanInterval();
            }

            @Override
            public boolean onException(Throwable e) {
                logger.error("cleanup error.", e);
                return true;
            }
        }, "JMQ_SERVER_CLEANUP_THREAD");

        cleanupThread.start();
        logger.info("Cleanup service is started.");
    }

    @Override
    protected void doStop() {
        Close.close(cleanupThread);
        cleanupThread = null;
        super.doStop();
        logger.info("Cleanup service is stopped.");
    }

    /**
     * 清理
     */
    protected void cleanup() throws Exception {
        if (!isStarted()) {
            return;
        }
        // 尝试加锁，避免已经清理了
        boolean isLocked = writeLock.tryLock();
        if (isLocked) {
            try {
                Map<String, Long> minAckOffsets = new HashMap<String, Long>();
                Map<String, TopicOffset> offsetMap = offsetManager.getOffset();
                if (offsetMap != null && offsetMap.values() != null) {
                    for (TopicOffset topicOffset : offsetMap.values()) {
                        if (topicOffset != null && topicOffset.getOffsets() != null) {
                            for (Map.Entry<Integer, QueueOffset> entry : topicOffset.getOffsets().entrySet()) {
                                QueueOffset offset = entry.getValue();
                                String topic = offset.getTopic();
                                int queueId = offset.getQueueId();
                                long minAckOffset = offsetManager.getMinAckOffset(topic, queueId, -1l);
                                minAckOffsets.put(topic + ":" + queueId, minAckOffset);
                            }
                        }

                    }
                }
                store.cleanUp(minAckOffsets);
            } finally {
                writeLock.unlock();
            }
        }
    }
}