package com.ipd.jmq.server.store;


import com.ipd.jmq.server.context.ContextEvent;
import com.ipd.jmq.server.store.context.StoreContext;
import com.ipd.jmq.server.store.journal.JournalConfig;
import com.ipd.jmq.toolkit.concurrent.EventListener;

import java.io.File;

/**
 * 存储配置
 */
public class StoreConfig implements EventListener<ContextEvent> {
    // 队列文件镜像缓存配置
    protected JournalConfig queueConfig;
    // 日志文件镜像缓存配置
    protected JournalConfig journalConfig;

    //过期事务询问次数，无限制
    public static final int TX_QUERY_LIMIT_INFINITE = -1;
    //日志文件大小
    public static final int JOURNAL_FILE_LENGTH = 1024 * 1024 * 128;
    //队列文件大小
    public static final int QUEUE_FILE_LENGTH = 300000 * ConsumeQueue.CQ_RECORD_SIZE;

    //日志镜像文件占用最大内存空间(20G,160个日志文件)
    public static long JOURNAL_CACHE_SIZE = 21474836480L;
    //队列镜像文件占用最大内存空间(10G,,1626个队列文件)
    public static long QUEUE_CACHE_SIZE = 10737418240L;

    //缓存新生代比率(保留最热点的文件，超过则启动检查程序卸载)
    public static int CACHE_NEW_RATIO = 40;
    //缓存最大比率(超过该值，则不能缓存，可以设置为0不启用缓存)
    public static int CACHE_MAX_RATIO = 95;
    //缓存强制收回的比率阀值(超过则按照优先级强制收回，队列文件数也可以尽量保留少)
    public static int CACHE_EVICT_THRESHOLD = 90;
    //缓存一次强制收回的最小比率
    public static int CACHE_EVICT_MIN_RATIO = 10;
    //优先保留每个队列最近的文件数量
    public static int CACHE_QUEUE_FILES = 3;
    //缓存最大空闲时间，超过则会被移除到待清理队列
    public static int CACHE_MAX_IDLE = 1000 * 60 * 10;
    //检查缓存时间间隔
    public static int CACHE_CHECK_INTERVAL = 1000 * 2;
    //缓存延时回收的时间，为0则立即释放
    public static int CACHE_EVICT_LATENCY = 1000 * 10;
    //缓存存在内存泄露时间
    public static int CACHE_LEAK_TIME = 1000 * 180;

    //数据目录
    private File dataDirectory;
    //日志目录
    private File journalDirectory;
    //队列目录
    private File queueDirectory;
    //复制目录
    private File haDirectory;
    //归档日志目录
    private File archiveDirectory;
    //坏块目录
    private File badFileDirectory;
    //检查点文件
    private File checkPointFile;
    //异常关闭文件
    private File abortFile;
    //消费位置文件
    private File offsetFile;
    //订阅关系文件
    private File subscribeFile;
    //有效队列大小文件
    private File topicQueueFile;
    //锁文件
    private File lockFile;
    //统计文件
    private File statFile;

    //日志文件刷盘策略
    private FlushPolicy journalFlushPolicy = new FlushPolicy(true);
    //队列文件刷盘策略
    private FlushPolicy queueFlushPolicy = new FlushPolicy(false, 1000 * 60, 4096 * 2);

    //清理时间间隔
    private int cleanInterval = 1000 * 60;
    //删除多个文件时间间隔(毫秒)
    private int deleteFilesInterval = 100;
    //刷新queue文件间隔,不能太长，否则完成一次需要消耗太长时间
    private int queueFlushFileInterval = 10;
    //删除的时刻(<0表示任意时刻都可以删除，0-23表示时刻）
    private byte deleteTime = -1;
    //检查点时间(默认30秒)
    private int checkPoint = 1000 * 30;
    //刷盘和复制超时时间
    private int flushTimeout = 1000 * 2;
    //文件保留时间(<0表示不保留)
    private int fileReserveTime = 1000 * 60 * 60 * 48;
    //强制删除文件最小保留时间(1分钟)
    private long cleanupReserveTime = 60 * 1000;
    //限制的文件句柄总数
    private int fileLimit = (int) (65535 * 0.75);
    //强制清理文件的磁盘占用百分比阀值
    private byte cleanupWhenUsage = 75;

    //最大磁盘利用率(用于动态计算最大磁盘空间)
    private byte maxDiskSpaceUsage = 75;
    //最大磁盘空间
    private long maxDiskSpace = 0;
    //最大消息体大小 默认 2M
    private int maxMessageSize = 1024 * 1024 * 2;
    //最大批量大小
    private short maxBatchSize = 100;
    //块大小，要远大于最大消息体大小，用于查找有效数据等
    private int blockSize = 1024 * 1024 * 5;
    //派发队列流量控制阀值
    private int dispatchFlowThreshold = 600000;
    //默认队列数
    private short defaultQueues = 3;
    //异常退出恢复队列文件数量
    private int recoverQueueFiles = 3;
    //恢复的时候校验和
    private boolean checksumOnRecover = true;

    //归档日志Ringbuffer容量
    private int archiveCapacity = 100000;
    //归档日志缓冲区大小(2M+1K)
    private int archiveBufferSize = 2098176;
    //归档日志生产刷盘的数量阀值
    private int archiveProduceThreshold = 5000;
    //归档日志消费刷盘的数量阀值
    private int archiveConsumeThreshold = 3000;
    //归档日志消费消息入队超时(线上100比50好)
    private int archiveConsumeEnqueueTimeout = 100;
    //归档日志生产消息入队超时
    private int archiveProduceEnqueueTimeout = 5;
    //归档生产日志文件大小(默认5M)
    private long archiveProduceFileSize = 5 * 1024 * 1024;
    //归档消费日志文件大小(默认32M)
    private long archiveConsumeFileSize = 32 * 1024 * 1024;
    //归档切换文件时间间隔(默认5分钟)
    private int archiveRollingInterval = 1000 * 60 * 5;

    //重新派发队列大小
    private int redispatchQueueSize = 100;
    //消息序列化缓冲区大小
    private int bufferPoolSize = 1000;
    //消息等待写入队列大小
    private int writeQueueSize = 30000;
    //消息入队超时时间(线上100比50好)
    private int writeEnqueueTimeout = 100;
    //刷盘队列大小
    private int commitQueueSize = 8192;
    //入刷盘队列超时时间
    private int commitEnqueueTimeout = 0;
    //建索引队列大小
    private int dispatchQueueSize = 40000;
    //入建索引队列超时时间
    private int dispatchEnqueueTimeout = 0;

    // 广播确认时间戳
    private long broadcastOffsetAckTimeDifference = 1000 * 60 * 60 * 24 * 2;
    // 广播确认时间间隔
    private long broadcastOffsetAckInterval = 1000 * 60 * 20;
    //忽略journal
    private boolean ignoreJournalError = false;
    //事务保留时间
    private int transactionReserveTime = 1000 * 60 * 60 * 48;
    //过期事务询问次数(成功次数)
    private int transactionQueryLimit = TX_QUERY_LIMIT_INFINITE;
    //过期事务反查被加锁超时间隔
    private int transactionFeadbackLockedTimeout = 1000 * 60;
    //事务限制数
    private int transactionLimit = 1000;
    //事务需要建索引消息大小下限(字节)
    private int transactionNeedDispatchSize = 200;

    /**
     * 构造函数
     *
     * @param dataDirectory 数据目录
     */
    public StoreConfig(File dataDirectory) {
        if (dataDirectory == null) {
            throw new IllegalArgumentException("dataDirectory can not be null");
        }
        if (dataDirectory.exists()) {
            if (!dataDirectory.isDirectory()) {
                throw new IllegalArgumentException(String.format("%s is not a directory", dataDirectory.getPath()));
            }
        } else {
            if (!dataDirectory.mkdirs()) {
                if (!dataDirectory.exists()) {
                    throw new IllegalArgumentException(
                            String.format("create directory %s error.", dataDirectory.getPath()));
                }
            }
        }
        if (!dataDirectory.canWrite()) {
            throw new IllegalArgumentException(String.format("%s can not be written", dataDirectory.getPath()));
        }
        if (!dataDirectory.canRead()) {
            throw new IllegalArgumentException(String.format("%s can not be read", dataDirectory.getPath()));
        }

        this.dataDirectory = dataDirectory;
        this.journalDirectory = new File(dataDirectory, "journal/");
        this.queueDirectory = new File(dataDirectory, "queue/");
        this.haDirectory = new File(dataDirectory, "replication/");
        this.archiveDirectory = new File(dataDirectory, "archive/");
        this.badFileDirectory = new File(dataDirectory, "badfile/");

        this.checkPointFile = new File(dataDirectory, "checkpoint");
        this.abortFile = new File(dataDirectory, "abort");
        this.offsetFile = new File(dataDirectory, "offset");
        this.subscribeFile = new File(dataDirectory, "subscribe");
        this.topicQueueFile = new File(dataDirectory, "queues");
        this.lockFile = new File(dataDirectory, "lock");
        this.statFile = new File(dataDirectory, "stat");

        if (!journalDirectory.exists()) {
            journalDirectory.mkdir();
        }
        if (!queueDirectory.exists()) {
            queueDirectory.mkdir();
        }
        if (!haDirectory.exists()) {
            haDirectory.mkdir();
        }
        if (!archiveDirectory.exists()) {
            archiveDirectory.mkdir();
        }
    }

    public StoreConfig(String file) {
        this(new File(file));
    }

    public File getDataDirectory() {
        return this.dataDirectory;
    }

    public File getJournalDirectory() {
        return this.journalDirectory;
    }

    public File getQueueDirectory() {
        return this.queueDirectory;
    }

    public File getCheckPointFile() {
        return this.checkPointFile;
    }

    public File getAbortFile() {
        return this.abortFile;
    }

    public File getOffsetFile() {
        return this.offsetFile;
    }

    public File getTopicQueueFile() {
        return topicQueueFile;
    }

    public File getSubscribeFile() {
        return subscribeFile;
    }

    public File getHaDirectory() {
        return haDirectory;
    }

    public File getLockFile() {
        return lockFile;
    }

    public File getStatFile() {
        return statFile;
    }

    public File getBadFileDirectory() {
        return badFileDirectory;
    }


    public FlushPolicy getJournalFlushPolicy() {
        return this.journalFlushPolicy;
    }

    public void setJournalFlushPolicy(FlushPolicy journalFlushPolicy) {
        if (journalFlushPolicy != null) {
            this.journalFlushPolicy = journalFlushPolicy;
        }
    }

    public FlushPolicy getQueueFlushPolicy() {
        return this.queueFlushPolicy;
    }

    public void setQueueFlushPolicy(FlushPolicy queueFlushPolicy) {
        if (queueFlushPolicy != null) {
            this.queueFlushPolicy = queueFlushPolicy;
        }
    }

    public int getCleanInterval() {
        return this.cleanInterval;
    }

    public void setCleanInterval(int cleanInterval) {
        if (cleanInterval > 0) {
            this.cleanInterval = cleanInterval;
        }
    }

    public int getDeleteFilesInterval() {
        return this.deleteFilesInterval;
    }

    public void setDeleteFilesInterval(int deleteFilesInterval) {
        if (deleteFilesInterval > 0) {
            this.deleteFilesInterval = deleteFilesInterval;
        }
    }

    public int getQueueFlushFileInterval() {
        return queueFlushFileInterval;
    }

    public void setQueueFlushFileInterval(int queueFlushFileInterval) {
        this.queueFlushFileInterval = queueFlushFileInterval;
    }

    public byte getDeleteTime() {
        return this.deleteTime;
    }

    public void setDeleteTime(byte deleteTime) {
        if (deleteTime < 24) {
            this.deleteTime = deleteTime;
        }
    }

    public int getFileReserveTime() {
        return this.fileReserveTime;
    }

    public void setFileReserveTime(int fileReserveTime) {
        this.fileReserveTime = fileReserveTime;
    }

    public int getCheckPoint() {
        return checkPoint;
    }

    public void setCheckPoint(int checkPoint) {
        if (checkPoint > 0) {
            this.checkPoint = checkPoint;
        }
    }

    public int getFlushTimeout() {
        return flushTimeout;
    }

    public void setFlushTimeout(int flushTimeout) {
        if (flushTimeout > 0) {
            this.flushTimeout = flushTimeout;
        }
    }

    public byte getCleanupWhenUsage() {
        return cleanupWhenUsage;
    }

    public void setCleanupWhenUsage(byte cleanupWhenUsage) {
        if (cleanupWhenUsage > 0 && cleanupWhenUsage <= 100) {
            this.cleanupWhenUsage = cleanupWhenUsage;
        }
    }

    public byte getMaxDiskSpaceUsage() {
        return this.maxDiskSpaceUsage;
    }

    public void setMaxDiskSpaceUsage(byte maxDiskSpaceUsage) {
        if (maxDiskSpaceUsage > 0 && maxDiskSpaceUsage <= 100) {
            this.maxDiskSpaceUsage = maxDiskSpaceUsage;
        }
    }

    public long getMaxDiskSpace() {
        return maxDiskSpace;
    }

    public void setMaxDiskSpace(long maxDiskSpace) {
        this.maxDiskSpace = maxDiskSpace;
    }

    public int getMaxMessageSize() {
        return this.maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        if (maxMessageSize > 0) {
            this.maxMessageSize = maxMessageSize;
        }
    }

    public short getMaxBatchSize() {
        return this.maxBatchSize;
    }

    public void setMaxBatchSize(short maxBatchSize) {
        if (maxBatchSize > 0) {
            this.maxBatchSize = maxBatchSize;
        }
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        if (blockSize > 0) {
            this.blockSize = blockSize;
        }
    }

    public int getDispatchFlowThreshold() {
        return this.dispatchFlowThreshold;
    }

    public void setDispatchFlowThreshold(int dispatchFlowThreshold) {
        if (dispatchFlowThreshold > 0) {
            this.dispatchFlowThreshold = dispatchFlowThreshold;
        }
    }

    public short getDefaultQueues() {
        return this.defaultQueues;
    }

    public void setDefaultQueues(short defaultQueues) {
        if (defaultQueues > 0) {
            this.defaultQueues = defaultQueues;
        }
    }

    public boolean isChecksumOnRecover() {
        return this.checksumOnRecover;
    }

    public void setChecksumOnRecover(boolean checksumOnRecover) {
        this.checksumOnRecover = checksumOnRecover;
    }

    public int getRecoverQueueFiles() {
        return recoverQueueFiles;
    }

    public void setRecoverQueueFiles(int recoverQueueFiles) {
        if (recoverQueueFiles > 0) {
            this.recoverQueueFiles = recoverQueueFiles;
        }
    }

    public File getArchiveDirectory() {
        return archiveDirectory;
    }

    public int getArchiveCapacity() {
        return archiveCapacity;
    }

    public void setArchiveCapacity(int archiveCapacity) {
        if (archiveCapacity > 0) {
            this.archiveCapacity = archiveCapacity;
        }
    }

    public int getArchiveBufferSize() {
        return archiveBufferSize;
    }

    public void setArchiveBufferSize(int archiveBufferSize) {
        if (archiveBufferSize > 0) {
            this.archiveBufferSize = archiveBufferSize;
        }
    }

    public int getArchiveProduceThreshold() {
        return archiveProduceThreshold;
    }

    public void setArchiveProduceThreshold(int archiveProduceThreshold) {
        if (archiveProduceThreshold > 0) {
            this.archiveProduceThreshold = archiveProduceThreshold;
        }
    }

    public int getArchiveConsumeThreshold() {
        return archiveConsumeThreshold;
    }

    public void setArchiveConsumeThreshold(int archiveConsumeThreshold) {
        if (archiveConsumeThreshold > 0) {
            this.archiveConsumeThreshold = archiveConsumeThreshold;
        }
    }

    public int getArchiveConsumeEnqueueTimeout() {
        return archiveConsumeEnqueueTimeout;
    }

    public void setArchiveConsumeEnqueueTimeout(int archiveConsumeEnqueueTimeout) {
        if (archiveConsumeEnqueueTimeout > 0) {
            this.archiveConsumeEnqueueTimeout = archiveConsumeEnqueueTimeout;
        }
    }

    public int getArchiveProduceEnqueueTimeout() {
        return archiveProduceEnqueueTimeout;
    }

    public void setArchiveProduceEnqueueTimeout(int archiveProduceEnqueueTimeout) {
        if (archiveProduceEnqueueTimeout > 0) {
            this.archiveProduceEnqueueTimeout = archiveProduceEnqueueTimeout;
        }
    }

    public long getArchiveProduceFileSize() {
        return archiveProduceFileSize;
    }

    public void setArchiveProduceFileSize(long archiveProduceFileSize) {
        if (archiveProduceFileSize > 0) {
            this.archiveProduceFileSize = archiveProduceFileSize;
        }
    }

    public long getArchiveConsumeFileSize() {
        return archiveConsumeFileSize;
    }

    public void setArchiveConsumeFileSize(long archiveConsumeFileSize) {
        if (archiveConsumeFileSize > 0) {
            this.archiveConsumeFileSize = archiveConsumeFileSize;
        }
    }

    public int getArchiveRollingInterval() {
        return archiveRollingInterval;
    }

    public void setArchiveRollingInterval(int archiveRollingInterval) {
        if (archiveRollingInterval > 0) {
            this.archiveRollingInterval = archiveRollingInterval;
        }
    }

    public int getDispatchEnqueueTimeout() {
        return dispatchEnqueueTimeout;
    }

    public void setDispatchEnqueueTimeout(int dispatchEnqueueTimeout) {
        if (dispatchEnqueueTimeout > 0) {
            this.dispatchEnqueueTimeout = dispatchEnqueueTimeout;
        }
    }

    public int getDispatchQueueSize() {
        return dispatchQueueSize;
    }

    public void setDispatchQueueSize(int dispatchQueueSize) {
        if (dispatchQueueSize > 0) {
            this.dispatchQueueSize = dispatchQueueSize;
        }
    }

    public int getCommitEnqueueTimeout() {
        return commitEnqueueTimeout;
    }

    public void setCommitEnqueueTimeout(int commitEnqueueTimeout) {
        if (commitEnqueueTimeout > 0) {
            this.commitEnqueueTimeout = commitEnqueueTimeout;
        }
    }

    public int getCommitQueueSize() {
        return commitQueueSize;
    }

    public void setCommitQueueSize(int commitQueueSize) {
        if (commitQueueSize > 0) {
            this.commitQueueSize = commitQueueSize;
        }
    }

    public int getWriteEnqueueTimeout() {
        return writeEnqueueTimeout;
    }

    public void setWriteEnqueueTimeout(int writeEnqueueTimeout) {
        if (writeEnqueueTimeout > 0) {
            this.writeEnqueueTimeout = writeEnqueueTimeout;
        }
    }

    public int getRedispatchQueueSize() {
        return redispatchQueueSize;
    }

    public void setRedispatchQueueSize(int redispatchQueueSize) {
        if (redispatchQueueSize > 0) {
            this.redispatchQueueSize = redispatchQueueSize;
        }
    }

    public int getBufferPoolSize() {
        return bufferPoolSize;
    }

    public void setBufferPoolSize(int bufferPoolSize) {
        if (bufferPoolSize > 0) {
            this.bufferPoolSize = bufferPoolSize;
        }
    }

    public int getWriteQueueSize() {
        return writeQueueSize;
    }

    public void setWriteQueueSize(int writeQueueSize) {
        if (writeQueueSize > 0) {
            this.writeQueueSize = writeQueueSize;
        }
    }

    public int getFileLimit() {
        return fileLimit;
    }

    public void setFileLimit(int fileLimit) {
        this.fileLimit = fileLimit;
    }

    public long getCleanupReserveTime() {
        return cleanupReserveTime;
    }

    public void setCleanupReserveTime(long cleanupReserveTime) {
        this.cleanupReserveTime = cleanupReserveTime;
    }

    public long getBroadcastOffsetAckTimeDifference() {
        return broadcastOffsetAckTimeDifference;
    }

    public void setBroadcastOffsetAckTimeDifference(long broadcastOffsetAckTimeDifference) {
        this.broadcastOffsetAckTimeDifference = broadcastOffsetAckTimeDifference;
    }

    public long getBroadcastOffsetAckInterval() {
        return broadcastOffsetAckInterval;
    }

    public void setBroadcastOffsetAckInterval(long broadcastOffsetAckInterval) {
        this.broadcastOffsetAckInterval = broadcastOffsetAckInterval;
    }

    public boolean isIgnoreJournalError() {
        return ignoreJournalError;
    }

    public void setIgnoreJournalError(boolean ignoreJournalError) {
        this.ignoreJournalError = ignoreJournalError;
    }

    public int getTransactionQueryLimit() {
        return transactionQueryLimit;
    }

    public void setTransactionQueryLimit(int transactionQueryLimit) {
        this.transactionQueryLimit = transactionQueryLimit;
    }

    public int getTransactionReserveTime() {
        return transactionReserveTime;
    }

    public void setTransactionReserveTime(int transactionReserveTime) {
        this.transactionReserveTime = transactionReserveTime;
    }

    public int getTransactionFeadbackLockedTimeout() {
        return transactionFeadbackLockedTimeout;
    }

    public void setTransactionFeadbackLockedTimeout(int transactionFeadbackLockedTimeout) {
        this.transactionFeadbackLockedTimeout = transactionFeadbackLockedTimeout;
    }

    public int getTransactionLimit() {
        return transactionLimit;
    }

    public void setTransactionLimit(int transactionLimit) {
        this.transactionLimit = transactionLimit;
    }

    public int getTransactionNeedDispatchSize() {
        return transactionNeedDispatchSize;
    }

    public void setTransactionNeedDispatchSize(int transactionNeedDispatchSize) {
        this.transactionNeedDispatchSize = transactionNeedDispatchSize;
    }

    public JournalConfig getQueueConfig() {
        return queueConfig;
    }

    public void setQueueConfig(JournalConfig queueConfig) {
        this.queueConfig = queueConfig;
    }

    public JournalConfig getJournalConfig() {
        return journalConfig;
    }

    public void setJournalConfig(JournalConfig journalConfig) {
        this.journalConfig = journalConfig;
    }

    @Override
    public void onEvent(ContextEvent event) {
        if (event == null) {
            return;
        }

        String key = event.getKey();
        if (StoreContext.STORE_CLEAN_INTERVAL.equals(key)) {
            setCleanInterval(event.getPositive(cleanInterval));
        } else if (StoreContext.STORE_DELETE_FILES_INTERVAL.equals(key)) {
            setDeleteFilesInterval(event.getPositive(deleteFilesInterval));
        } else if (StoreContext.STORE_DELETE_TIME.equals(key)) {
            setDeleteTime(event.getByte(deleteTime));
        } else if (StoreContext.STORE_CHECKPOINT.equals(key)) {
            setCheckPoint(event.getPositive(checkPoint));
        } else if (StoreContext.STORE_FLUSH_TIMEOUT.equals(key)) {
            setFlushTimeout(event.getPositive(flushTimeout));
        } else if (StoreContext.STORE_FILE_RESERVE_Time.equals(key)) {
            setFileReserveTime(event.getInt(fileReserveTime));
        } else if (StoreContext.STORE_CLEANUP_WHEN_USAGE.equals(key)) {
            setCleanupWhenUsage(event.getPositive(cleanupWhenUsage));
        } else if (StoreContext.STORE_MAX_DISK_SPACE_USAGE.equals(key)) {
            setMaxDiskSpaceUsage(event.getPositive(maxDiskSpaceUsage));
        } else if (StoreContext.STORE_MAX_DISK_SPACE.equals(key)) {
            setMaxDiskSpace(event.getNatural(maxDiskSpace));
        } else if (StoreContext.STORE_MAX_MESSAGE_SIZE.equals(key)) {
            setMaxMessageSize(event.getPositive(maxMessageSize));
        } else if (StoreContext.STORE_MAX_BATCH_SIZE.equals(key)) {
            setMaxBatchSize(event.getPositive(maxBatchSize));
        } else if (StoreContext.STORE_DISPATCH_FLOW_THRESHOLD.equals(key)) {
            setDispatchFlowThreshold(event.getPositive(dispatchFlowThreshold));
        } else if (StoreContext.STORE_DEFAULT_QUEUES.equals(key)) {
            setDefaultQueues(event.getPositive(defaultQueues));
        } else if (StoreContext.STORE_RECOVER_QUEUE_FILES.equals(key)) {
            setRecoverQueueFiles(event.getPositive(recoverQueueFiles));
        } else if (StoreContext.STORE_CHECKSUM_ON_RECOVER.equals(key)) {
            setChecksumOnRecover(event.getBoolean(checksumOnRecover));
        } else if (StoreContext.STORE_JOURNAL_CACHE_SIZE.equals(key)) {
            journalConfig.setCacheSize(event.getPositive(JOURNAL_CACHE_SIZE));
        } else if (StoreContext.STORE_QUEUE_CACHE_SIZE.equals(key)) {
            queueConfig.setCacheSize(event.getPositive(QUEUE_CACHE_SIZE));
        } else if (StoreContext.STORE_CACHE_NEW_RATIO.equals(key)) {
            journalConfig.setCacheNewRatio(event.getPositive(CACHE_NEW_RATIO));
            queueConfig.setCacheNewRatio(event.getPositive(CACHE_NEW_RATIO));
        } else if (StoreContext.STORE_CACHE_MAX_RATIO.equals(key)) {
            journalConfig.setCacheMaxRatio(event.getPositive(CACHE_MAX_RATIO));
            queueConfig.setCacheMaxRatio(event.getPositive(CACHE_MAX_RATIO));
        } else if (StoreContext.STORE_CACHE_EVICT_THRESHOLD.equals(key)) {
            journalConfig.setCacheEvictThreshold(event.getPositive(CACHE_EVICT_THRESHOLD));
            queueConfig.setCacheEvictThreshold(event.getPositive(CACHE_EVICT_THRESHOLD));
        } else if (StoreContext.STORE_CACHE_EVICT_MIN_RATIO.equals(key)) {
            journalConfig.setCacheEvictMinRatio(event.getPositive(CACHE_EVICT_MIN_RATIO));
            queueConfig.setCacheEvictMinRatio(event.getPositive(CACHE_EVICT_MIN_RATIO));
        } else if (StoreContext.STORE_CACHE_QUEUE_FILES.equals(key)) {
            journalConfig.setCacheFiles(event.getPositive(CACHE_QUEUE_FILES));
            queueConfig.setCacheFiles(event.getPositive(CACHE_QUEUE_FILES));
        } else if (StoreContext.STORE_CACHE_MAX_IDLE.equals(key)) {
            journalConfig.setCacheMaxIdle(event.getPositive(CACHE_MAX_IDLE));
            queueConfig.setCacheMaxIdle(event.getPositive(CACHE_MAX_IDLE));
        } else if (StoreContext.STORE_CACHE_CHECK_INTERVAL.equals(key)) {
            journalConfig.setCacheCheckInterval(event.getPositive(CACHE_CHECK_INTERVAL));
            queueConfig.setCacheCheckInterval(event.getPositive(CACHE_CHECK_INTERVAL));
        } else if (StoreContext.STORE_CACHE_EVICT_LATENCY.equals(key)) {
            journalConfig.setCacheEvictLatency(event.getPositive(CACHE_EVICT_LATENCY));
            queueConfig.setCacheEvictLatency(event.getPositive(CACHE_EVICT_LATENCY));
        } else if (StoreContext.STORE_CACHE_LEAK_TIME.equals(key)) {
            journalConfig.setCacheLeakTime(event.getPositive(CACHE_LEAK_TIME));
            queueConfig.setCacheLeakTime(event.getPositive(CACHE_LEAK_TIME));
        } else if (StoreContext.STORE_REDISPATCH_QUEUE_SIZE.equals(key)) {
            setRedispatchQueueSize(event.getPositive(redispatchQueueSize));
        } else if (StoreContext.STORE_BUFFER_POOL_SIZE.equals(key)) {
            setBufferPoolSize(event.getPositive(bufferPoolSize));
        } else if (StoreContext.STORE_WRITE_QUEUE_SIZE.equals(key)) {
            setWriteQueueSize(event.getPositive(writeQueueSize));
        } else if (StoreContext.STORE_WRITE_ENQUEUE_TIMEOUT.equals(key)) {
            setWriteEnqueueTimeout(event.getPositive(writeEnqueueTimeout));
        } else if (StoreContext.STORE_COMMIT_QUEUE_SIZE.equals(key)) {
            setCommitQueueSize(event.getPositive(commitQueueSize));
        } else if (StoreContext.STORE_COMMIT_ENQUEUE_TIMEOUT.equals(key)) {
            setCommitEnqueueTimeout(event.getPositive(commitEnqueueTimeout));
        } else if (StoreContext.STORE_DISPATCH_QUEUE_SIZE.equals(key)) {
            setDispatchQueueSize(event.getPositive(dispatchQueueSize));
        } else if (StoreContext.STORE_DISPATCH_ENQUEUE_TIMEOUT.equals(key)) {
            setDispatchEnqueueTimeout(event.getPositive(dispatchEnqueueTimeout));
        } else if (StoreContext.STORE_FILE_LIMIT.equals(key)) {
            setFileLimit(event.getPositive(fileLimit));
        } else if (StoreContext.ARCHIVE_CAPACITY.equals(key)) {
            setArchiveCapacity(event.getPositive(archiveCapacity));
        } else if (StoreContext.ARCHIVE_PRODUCE_THRESHOLD.equals(key)) {
            setArchiveProduceThreshold(event.getPositive(archiveProduceThreshold));
        } else if (StoreContext.ARCHIVE_CONSUME_THRESHOLD.equals(key)) {
            setArchiveConsumeThreshold(event.getPositive(archiveConsumeThreshold));
        } else if (StoreContext.ARCHIVE_PRODUCE_FILE_SIZE.equals(key)) {
            setArchiveProduceFileSize(event.getPositive(archiveProduceFileSize));
        } else if (StoreContext.ARCHIVE_CONSUME_FILE_SIZE.equals(key)) {
            setArchiveConsumeFileSize(event.getPositive(archiveConsumeFileSize));
        } else if (StoreContext.ARCHIVE_ROLLING_INTERVAL.equals(key)) {
            setArchiveRollingInterval(event.getPositive(archiveRollingInterval));
        } else if (StoreContext.ARCHIVE_PRODUCE_ENQUEUE_TIMEOUT.equals(key)) {
            setArchiveProduceEnqueueTimeout(event.getPositive(archiveProduceEnqueueTimeout));
        } else if (StoreContext.ARCHIVE_CONSUME_ENQUEUE_TIMEOUT.equals(key)) {
            setArchiveConsumeEnqueueTimeout(event.getPositive(archiveConsumeEnqueueTimeout));
        } else if (StoreContext.BROADCAST_OFFSET_ACK_INTERVAL.equals(key)) {
            setBroadcastOffsetAckInterval(event.getPositive(broadcastOffsetAckInterval));
        } else if (StoreContext.BROADCAST_OFFSET_ACK_TIME_DIFFERENCE.equals(key)) {
            setBroadcastOffsetAckTimeDifference(event.getPositive(broadcastOffsetAckTimeDifference));
        } else if (StoreContext.STORE_JOURNAL_FLUSH_SYNC.equals(key)) {
            journalFlushPolicy.setSync(event.getBoolean(journalFlushPolicy.isSync()));
        } else if (StoreContext.STORE_JOURNAL_FLUSH_TIMEOUT.equals(key)) {
            journalFlushPolicy.setSyncTimeout(event.getPositive(journalFlushPolicy.getSyncTimeout()));
        } else if (StoreContext.STORE_JOURNAL_FLUSH_INTERVAL.equals(key)) {
            journalFlushPolicy.setFlushInterval(event.getPositive(journalFlushPolicy.getFlushInterval()));
        } else if (StoreContext.STORE_JOURNAL_FLUSH_SIZE.equals(key)) {
            journalFlushPolicy.setDataSize(event.getPositive(journalFlushPolicy.getDataSize()));
        } else if (StoreContext.STORE_JOURNAL_FLUSH_CHECK_INTERVAL.equals(key)) {
            journalFlushPolicy.setCheckInterval(event.getPositive(journalFlushPolicy.getCheckInterval()));
        } else if (StoreContext.STORE_QUEUE_FLUSH_SYNC.equals(key)) {
            queueFlushPolicy.setSync(event.getBoolean(queueFlushPolicy.isSync()));
        } else if (StoreContext.STORE_QUEUE_FLUSH_TIMEOUT.equals(key)) {
            queueFlushPolicy.setSyncTimeout(event.getPositive(queueFlushPolicy.getSyncTimeout()));
        } else if (StoreContext.STORE_QUEUE_FLUSH_INTERVAL.equals(key)) {
            queueFlushPolicy.setFlushInterval(event.getPositive(queueFlushPolicy.getFlushInterval()));
        } else if (StoreContext.STORE_QUEUE_FLUSH_FILE_INTERVAL.equals(key)) {
            setQueueFlushFileInterval(event.getPositive(getQueueFlushFileInterval()));
        } else if (StoreContext.STORE_QUEUE_FLUSH_SIZE.equals(key)) {
            queueFlushPolicy.setDataSize(event.getPositive(queueFlushPolicy.getDataSize()));
        } else if (StoreContext.STORE_QUEUE_FLUSH_CHECK_INTERVAL.equals(key)) {
            queueFlushPolicy.setCheckInterval(event.getPositive(queueFlushPolicy.getCheckInterval()));
        } else if (StoreContext.STORE_TRANSACTION_RESERVE_TIME.equals(key)) {
            setTransactionReserveTime(event.getInt(transactionReserveTime));
        } else if (StoreContext.STORE_TRANSACTION_QUERY_LIMIT.equals(key)) {
            setTransactionQueryLimit(event.getInt(transactionQueryLimit));
        } else if (StoreContext.STORE_TRANSACTION_FEEDBACK_LOCKED_TIMEOUT.equals(key)) {
            setTransactionFeadbackLockedTimeout(event.getInt(transactionFeadbackLockedTimeout));
        } else if (StoreContext.STORE_TRANSACTION_LIMIT.equals(key)) {
            setTransactionLimit(event.getInt(transactionLimit));
        } else if (StoreContext.STORE_TRANSACTION_NEED_DISPATCH_SIZE.equals(key)) {
            setTransactionNeedDispatchSize(event.getInt(transactionNeedDispatchSize));
        }
    }
}