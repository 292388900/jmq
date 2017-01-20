package com.ipd.jmq.server.store.context;

import com.ipd.jmq.server.context.ContextEvent;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lindeqiang
 * @since 2016/8/19 14:20
 */
public class StoreContext extends ContextEvent {
    public static final String STORE_CLEAN_INTERVAL = "store.cleanInterval";
    public static final String STORE_DELETE_FILES_INTERVAL = "store.deleteFilesInterval";
    public static final String STORE_DELETE_TIME = "store.deleteTime";
    public static final String STORE_CHECKPOINT = "store.checkPoint";
    public static final String STORE_FLUSH_TIMEOUT = "store.flushTimeout";
    public static final String STORE_FILE_RESERVE_Time = "store.fileReserveHours";
    public static final String STORE_CLEANUP_WHEN_USAGE = "store.cleanupWhenUsage";
    public static final String STORE_MAX_DISK_SPACE_USAGE = "store.maxDiskSpaceUsage";
    public static final String STORE_MAX_DISK_SPACE = "store.maxDiskSpace";
    public static final String STORE_MAX_MESSAGE_SIZE = "store.maxMessageSize";
    public static final String STORE_MAX_BATCH_SIZE = "store.maxBatchSize";
    public static final String STORE_DISPATCH_FLOW_THRESHOLD = "store.dispatchFlowThreshold";
    public static final String STORE_DEFAULT_QUEUES = "store.defaultQueues";
    public static final String STORE_TRANSACTION_RESERVE_TIME = "store.transactionReserveTime";
    public static final String STORE_TRANSACTION_QUERY_LIMIT = "store.transactionQueryLimit";
    public static final String STORE_TRANSACTION_FEEDBACK_LOCKED_TIMEOUT = "store.transactionFeadbackLockedTimeout";
    public static final String STORE_TRANSACTION_LIMIT = "store.transactionLimit";
    public static final String STORE_TRANSACTION_NEED_DISPATCH_SIZE = "store.transactionNeedDispatchSize";

    public static final String STORE_RECOVER_QUEUE_FILES = "store.recoverQueueFiles";
    public static final String STORE_CHECKSUM_ON_RECOVER = "store.checksumOnRecover";
    public static final String STORE_JOURNAL_CACHE_SIZE = "store.journalCacheSize";
    public static final String STORE_QUEUE_CACHE_SIZE = "store.queueCacheSize";
    public static final String STORE_CACHE_NEW_RATIO = "store.cacheNewRatio";
    public static final String STORE_CACHE_MAX_RATIO = "store.cacheMaxRatio";
    public static final String STORE_CACHE_EVICT_THRESHOLD = "store.cacheEvictThreshold";
    public static final String STORE_CACHE_EVICT_MIN_RATIO = "store.cacheEvictMinRatio";
    public static final String STORE_CACHE_QUEUE_FILES = "store.cacheQueueFiles";
    public static final String STORE_CACHE_MAX_IDLE = "store.cacheMaxIdle";
    public static final String STORE_CACHE_CHECK_INTERVAL = "store.cacheCheckInterval";
    public static final String STORE_CACHE_EVICT_LATENCY = "store.cacheEvictLatency";
    public static final String STORE_CACHE_LEAK_TIME = "store.cacheLeakTime";

    public static final String STORE_REDISPATCH_QUEUE_SIZE = "store.redispatchQueueSize";
    public static final String STORE_BUFFER_POOL_SIZE = "store.bufferPoolSize";
    public static final String STORE_WRITE_QUEUE_SIZE = "store.writeQueueSize";
    public static final String STORE_WRITE_ENQUEUE_TIMEOUT = "store.writeEnqueueTimeout";
    public static final String STORE_COMMIT_QUEUE_SIZE = "store.commitQueueSize";
    public static final String STORE_COMMIT_ENQUEUE_TIMEOUT = "store.commitEnqueueTimeout";
    public static final String STORE_DISPATCH_QUEUE_SIZE = "store.dispatchQueueSize";
    public static final String STORE_DISPATCH_ENQUEUE_TIMEOUT = "store.dispatchEnqueueTimeout";
    public static final String STORE_FILE_LIMIT = "store.fileLimit";


    public static final String ARCHIVE_PRODUCE_THRESHOLD = "archive.produceThreshold";
    public static final String ARCHIVE_CAPACITY = "archive.capacity";
    public static final String ARCHIVE_CONSUME_THRESHOLD = "archive.consumeThreshold";
    public static final String ARCHIVE_PRODUCE_FILE_SIZE = "archive.produceFileSize";
    public static final String ARCHIVE_CONSUME_FILE_SIZE = "archive.consumeFileSize";
    public static final String ARCHIVE_ROLLING_INTERVAL = "archive.rollingInterval";
    public static final String ARCHIVE_PRODUCE_ENQUEUE_TIMEOUT = "archive.produceEnqueueTimeout";
    public static final String ARCHIVE_CONSUME_ENQUEUE_TIMEOUT = "archive.consumeEnqueueTimeout";

    public static final String STORE_JOURNAL_FLUSH_SYNC = "store.journalFlushSync";
    public static final String STORE_JOURNAL_FLUSH_TIMEOUT = "store.journalFlushTimeout";
    public static final String STORE_JOURNAL_FLUSH_INTERVAL = "store.journalFlushInterval";
    public static final String STORE_JOURNAL_FLUSH_SIZE = "store.journalFlushSize";
    public static final String STORE_JOURNAL_FLUSH_CHECK_INTERVAL = "store.journalFlushCheckInterval";

    public static final String STORE_QUEUE_FLUSH_SYNC = "store.queueFlushSync";
    public static final String STORE_QUEUE_FLUSH_TIMEOUT = "store.queueFlushTimeout";
    public static final String STORE_QUEUE_FLUSH_INTERVAL = "store.queueFlushInterval";
    public static final String STORE_QUEUE_FLUSH_FILE_INTERVAL = "store.queueFlushFileInterval";
    public static final String STORE_QUEUE_FLUSH_SIZE = "store.queueFlushSize";
    public static final String STORE_QUEUE_FLUSH_CHECK_INTERVAL = "store.queueFlushCheckInterval";

    public static final String BROADCAST_OFFSET_ACK_TIME_DIFFERENCE = "broadcast.OffsetAckTimeDifference";
    public static final String BROADCAST_OFFSET_ACK_INTERVAL = "broadcast.OffsetAckInterval";

    public StoreContext(String key, String value) {
        super(key, value);
    }
}
