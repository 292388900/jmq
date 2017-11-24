package com.ipd.jmq.server.store;


import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.*;
import com.ipd.jmq.common.model.JournalLog;
import com.ipd.jmq.common.network.ServerConfig;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.server.store.journal.AppendFile;
import com.ipd.jmq.server.store.journal.FileStat;
import com.ipd.jmq.toolkit.buffer.RByteBuffer;
import com.ipd.jmq.toolkit.cache.CacheCluster;
import com.ipd.jmq.toolkit.concurrent.EventBus;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.io.Files;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.service.ServiceThread;
import com.ipd.jmq.toolkit.time.SystemClock;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;

/**
 * 存储实现
 */
public class JMQStore extends Service implements Store, StoreUnsafe {

    public static final long LATEST_TIME = -1L;
    public static final long EARLIEST_TIME = -2L;
    public static String BACK_SUFFIX = ".1";
    private static Logger logger = LoggerFactory.getLogger(JMQStore.class);
    // 配置
    protected StoreConfig config;
    protected ServerConfig serverConfig;
    // Broker
    protected Broker broker;
    // 缓存
    protected CacheCluster cacheService;
    // 检查点
    protected CheckPoint checkPoint;
    // 日志管理器
    protected JournalManager journalManager;
    // 队列管理器
    protected QueueManager queueManager;
    // 派发服务
    protected DispatchService dispatchService;
    // 清理服务
    protected CleanupService cleanupService;
    // 检查点刷盘服务
    protected FlushManager flushManager;
    // 复制
    protected Replication replication;
    // 随机访问锁文件
    protected RandomAccessFile lockRaf;
    // 文件锁
    protected FileLock fileLock;
    // 事件管理器
    protected EventBus<StoreEvent> eventManager = new EventBus<StoreEvent>();
    // 异步重建消息索引服务
    protected RedispatchService redispatchService;

    // todo: 考虑slave不可用时会导致大量pendingMessages
    protected List<PendingMessage> pendingMessages = new ArrayList<PendingMessage>();
    protected Thread pendingMessageCheckThreadInstance;
    protected PendingMessageCheckThread pendingMessageCheckThread;

    // 已经复制并刷盘的JournalOffset
    private volatile long waterMark = 0;

    public static int restartTimes = 0;
    public static int restartSucces = 0;

    /**
     * 构造函数
     *
     * @param config       配置
     * @param broker       broker
     * @param cacheService 缓存
     */
    public JMQStore(StoreConfig config, Broker broker, CacheCluster cacheService) {
        if (config == null) {
            throw new IllegalArgumentException("config can not be null");
        }
        if (broker == null) {
            throw new IllegalArgumentException("broker can not be null");
        }
        this.config = config;
        this.broker = broker;
        this.cacheService = cacheService;
        this.pendingMessageCheckThread = new PendingMessageCheckThread(this);
        this.pendingMessageCheckThreadInstance = new Thread(pendingMessageCheckThread);
        this.pendingMessageCheckThreadInstance.setDaemon(true);
    }
    public  JMQStore(){
        this.pendingMessageCheckThread = new PendingMessageCheckThread(this);
        this.pendingMessageCheckThreadInstance = new Thread(pendingMessageCheckThread);
        this.pendingMessageCheckThreadInstance.setDaemon(true);
    }

    public void setBroker(Broker broker) {
        this.broker = broker;
    }

    public void setConfig(StoreConfig config) {
        this.config = config;
    }

    public void setServerConfig(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
        this.broker = new Broker(serverConfig.getIp(), serverConfig.getPort(), null);
        this.pendingMessageCheckThread = new PendingMessageCheckThread(this);
        this.pendingMessageCheckThreadInstance = new Thread(pendingMessageCheckThread);
        this.pendingMessageCheckThreadInstance.setDaemon(true);
    }

    public void setCacheService(CacheCluster cacheService) {
        this.cacheService = cacheService;
    }

    /**
     * 独占文件锁，确保只有一个实例启动
     *
     * @throws Exception
     */
    protected void lockFile() throws Exception {
        File lockFile = config.getLockFile();
        Files.createFile(lockFile);
        // 加文件锁
        lockRaf = new RandomAccessFile(lockFile, "rw");
        fileLock = lockRaf.getChannel().tryLock();
        if (fileLock == null) {
            throw new IOException(String.format("lock file error,%s", lockFile.getPath()));
        }
    }

    @Override
    protected void doStart() throws Exception {
        //重启计数
        restartTimes++;

        // 独占文件锁，确保只有一个实例启动
        lockFile();
        // 检查点
        checkPoint = new CheckPoint(config.getCheckPointFile());
        queueManager = new QueueManager(config);
        dispatchService = new DispatchService(config, queueManager, eventManager);
        journalManager = new JournalManager(config, broker, dispatchService, queueManager, eventManager);
        cleanupService = new CleanupService(config, journalManager, queueManager, checkPoint);
        redispatchService = new RedispatchService(config, journalManager, eventManager);
        journalManager.setCleanupService(cleanupService);
        flushManager = new FlushManager(config, checkPoint, journalManager, queueManager, this);
        eventManager.addListener(new EventListener<StoreEvent>() {
            @Override
            public void onEvent(StoreEvent event) {
                if (event.getType() == StoreEvent.EventType.FLUSHED) {
                    flushManager.setFlushedJournalOffset(event.getFlushedJournalOffset());
                }
            }
        });
        // 启动服务
        eventManager.start();
        checkPoint.start();
        journalManager.start();
        queueManager.start();
        dispatchService.start();
        // 恢复数据
        recover();

        // 检查点
        flushManager.start();
        // 清理服务
        cleanupService.start();
        redispatchService.setJournalOffset(journalManager.getMaxOffset());
        redispatchService.start();
        pendingMessageCheckThreadInstance.start();

        // 启动通知
        eventManager.inform(new StoreEvent(StoreEvent.EventType.START, null));

        config.getAbortFile().createNewFile();

        //计数
        restartSucces++;

        logger.info("store is started," + config.getJournalDirectory());
    }

    /**
     * 恢复日志和队列数据
     *
     * @throws Exception
     */
    protected void recover() throws Exception {
        // 判断是否正常退出
        boolean abort = config.getAbortFile().exists();
        if (abort) {
            logger.info("store was abnormal exited.");
        }
        // 计算恢复队列文件数量，正常退出恢复1个文件，否则恢复3个文件
        int recoverQueueFiles = !abort ? 1 : config.getRecoverQueueFiles();
        // 从检查点恢复日志文件偏移量
        long recoverJournalOffset = checkPoint.getRecoverOffset();
        // 恢复队列文件，得到需要恢复的日志文件起始位置
        long offset = queueManager.recover(recoverQueueFiles);

        // 计算最小的日志文件恢复位置
        if (offset >= 0 && offset < recoverJournalOffset) {
            recoverJournalOffset = offset;
            logger.info(String.format("recover journal from queue offset:%d", offset));
        } else {
            logger.info(String.format("recover journal from checkPoint offset:%d", recoverJournalOffset));
        }

        // 恢复日志
        journalManager.recover(recoverJournalOffset);
        // 删除错误的队列数据
        queueManager.truncate(journalManager.getMaxOffset());
    }

    @Override
    protected void doStop() {
        // 需要在日志文件和队列文件关闭之后才关闭清理服务，防止删除文件在睡眠，会等待很长时间
        Close.close(flushManager).close(journalManager).close(queueManager).close(checkPoint)
                .close(dispatchService).close(fileLock)
                .close(lockRaf).close(redispatchService);

        Files.deleteDirectory(config.getAbortFile());
        redispatchService = null;
        flushManager = null;
        cleanupService = null;
        journalManager = null;
        queueManager = null;
        checkPoint = null;
        dispatchService = null;
        fileLock = null;
        lockRaf = null;
        // 停止通知
        try {
            eventManager.inform(new StoreEvent(StoreEvent.EventType.STOP, null));
        } catch (Exception ignored) {
        }
        // 优雅关闭，把事件通知出去
        eventManager.stop(true);

        logger.info("store is stopped. " + config.getJournalDirectory());
    }

    @Override
    protected void beforeStop() {
        super.beforeStop();
        // 将要关闭，设置日志和队列服务状态为将要关闭，避免主从切换过程中，正在启动日志和队列服务，会阻塞较长时间
        JournalManager jm = journalManager;
        if (jm != null) {
            jm.willStop();
        }
        QueueManager qm = queueManager;
        if (qm != null) {
            qm.willStop();
        }
    }

    @Override
    public long getMaxOffset(final String topic, final int queueId) {
        return queueManager.getMaxOffset(topic, queueId);
    }

    @Override
    public long getMinOffset(final String topic, final int queueId) {
        return queueManager.getMinOffset(topic, queueId);
    }

    @Override
    public long getNextOffset(final String topic, final int queueId, final long offset) {
        return queueManager.getNextOffset(topic, queueId, offset);
    }

    public PutResult beginTransaction(BrokerPrepare prepare) throws JMQException {
        if (prepare == null) {
            return null;
        }
        if (!isStarted() || !isReady()) {
            throw new JMQException("store is stopped.", JMQCode.CN_SERVICE_NOT_AVAILABLE.getCode());
        }

        PutResult putResult = null;
        try {
            boolean isLimitedAddTransaction = journalManager.isLimitedAddTransaction(prepare.getTopic());
            if (!isLimitedAddTransaction) {
                putResult = putJournalLog(prepare);
            } else {
                throw new JMQException("transaction count whose topic is " + prepare.getTopic() + " is reached", JMQCode.FW_TRANSACTION_LIMIT.getCode());
            }
        } catch (Exception e) {
            logger.error("begin transaction error:" + prepare);
            if (e instanceof JMQException) {
                throw (JMQException) e;
            } else {
                throw new JMQException(JMQCode.SE_IO_ERROR, e);
            }
        }

        if (putResult != null && putResult.getCode() == JMQCode.SUCCESS) {
            journalManager.addTransaction(prepare);
        }
        return putResult;
    }


    public PutResult putTransactionMessages(List<BrokerMessage> preMessages) throws JMQException {
        if (preMessages == null || preMessages.isEmpty()) {
            return null;
        }
        if (!isStarted() || !isReady()) {
            throw new JMQException("store is stopped.", JMQCode.CN_SERVICE_NOT_AVAILABLE.getCode());
        }
        final List<BrokerRefMessage> refMessages = new ArrayList<BrokerRefMessage>();

        // 消息写盘后回调中处理加到prepare列表
        StoreContext storeContext = new StoreContext(null, null, preMessages, new ProcessedCallback<BrokerMessage>() {
            @Override
            public void onProcessed(StoreContext<BrokerMessage> context) throws JMQException {
                for (BrokerMessage preMessage : context.getLogs()) {
                    BrokerRefMessage refMessage = journalManager.createRefMsg(preMessage);
                    refMessages.add(refMessage);
                }
                BrokerPrepare prepare = journalManager.getBrokerPrepare(context.getLogs().get(0).getTxId());
                if (prepare == null) {
                    throw new JMQException("txid is not exists:" + context.getLogs().get(0).getTxId(), JMQCode.CN_UNKNOWN_ERROR.getCode());
                }
                prepare.addMessages(refMessages);
            }
        });

        storeContext.getResult().setCode(JMQCode.CN_UNKNOWN_ERROR);
        try {
            journalManager.appendJournalLog(storeContext);
        } catch (Exception e) {
            logger.error("add preMessage error:" + preMessages.get(0).getTxId() + ",topic" + preMessages.get(0).getTopic());
            if (e instanceof JMQException) {
                throw (JMQException) e;
            } else {
                throw new JMQException(JMQCode.SE_IO_ERROR, e);
            }
        }
        return storeContext.getResult();
    }

    public PutResult commitTransaction(BrokerCommit commit) throws JMQException {

        if (commit == null) {
            return null;
        }
        if (!isStarted() || !isReady()) {
            throw new JMQException("store is stopped.", JMQCode.CN_SERVICE_NOT_AVAILABLE.getCode());
        }

        PutResult result = null;

        JournalManager.InflightTx tx = journalManager.removeTransaction(commit.getTxId());
        boolean committed = false;
        try {
            if (tx == null || tx.getPrepare() == null) {
                throw new JMQException("txid is not exists or it was committed:" + commit.getTxId(), JMQCode
                        .CN_UNKNOWN_ERROR.getCode());
            }
            int msgCount = 0;
            for (BrokerRefMessage refMessage : tx.getPrepare().getMessages()) {
                PutResult curResult = putJournalLog(refMessage);
                if (curResult.getCode() != JMQCode.SUCCESS) {
                    throw new JMQException("write error:" + refMessage, JMQCode.SE_IO_ERROR.getCode());
                }
                msgCount++;
            }
            commit.setMsgCount(msgCount);
            result = putJournalLog(commit);
            committed = true;
        } finally {
            if (!committed && tx != null) {
                journalManager.addInflitTx(tx);
            }
        }

        return result;
    }

    public PutResult rollbackTransaction(BrokerRollback rollback) throws JMQException {
        if (rollback == null) {
            return null;
        }
        if (!isStarted() || !isReady()) {
            throw new JMQException("store is stopped.", JMQCode.CN_SERVICE_NOT_AVAILABLE.getCode());
        }

        PutResult putResult = null;
        try {
            putResult = putJournalLog(rollback);
        } catch (Exception e) {
            logger.error("rollback transaction error:" + rollback);
            if (e instanceof JMQException) {
                throw (JMQException) e;
            } else {
                throw new JMQException(JMQCode.SE_IO_ERROR, e);
            }
        }
        if (putResult != null && putResult.getCode() == JMQCode.SUCCESS) {
            journalManager.removeTransaction(rollback.getTxId());
        }
        return putResult;
    }

    @Override
    public void putJournalLogs(StoreContext<?> context) throws JMQException {
        if (context == null) {
            return;
        }
        try {
            final ProcessedCallback oldCallback = context.getCallback();
            if (oldCallback != null) {
                context.setCallback(new ProcessedCallback<BrokerMessage>() {
                    @Override
                    public void onProcessed(StoreContext<BrokerMessage> context) throws JMQException {
                        PutResult result = context.getResult();
                        if (result.getCode() == JMQCode.SUCCESS) {
                            List<MessageLocation> messageLocations = new ArrayList<MessageLocation>();
                            for (BrokerMessage message : context.getLogs()) {
                                messageLocations.add(new MessageLocation(message.getTopic(), message.getQueueId(), message.getQueueOffset()));
                            }
                            result.setLocation(messageLocations);
                        }
                        long journalOffset = context.getResult().getJournalOffset();
                        if (journalOffset < waterMark) {
                            oldCallback.onProcessed(context);
                        } else {
                            synchronized (pendingMessages) {
                                pendingMessages.add(new PendingMessage(journalOffset, context, oldCallback));
                            }
                            pendingMessageCheckThread.addTimeout(journalOffset, config.getFlushTimeout());
                        }
                    }
                });
            }
            journalManager.appendJournalLog(context);
        } catch (Exception e) {
            Message message = (Message) context.getLogs().get(0);
            if (context.getLogs().size() == 1) {
                // 单条存储出错
                logger.error(String.format("put message error. topic:%s,app:%s,businessId:%s", message.getTopic(),
                        message.getApp(), message.getBusinessId()), e);
            } else {
                // 批量存储出错
                logger.error(String.format("put batch messages error. topic:%s,app:%s", message.getTopic(), message.getApp()), e);
            }
            if (e instanceof JMQException) {
                throw (JMQException) e;
            }
            throw new JMQException(JMQCode.SE_IO_ERROR, e);
        }
    }

    @Override
    public <T extends JournalLog> PutResult putJournalLog(T log) throws JMQException {
        if (log == null) {
            return null;
        }

        List<T> logs = new ArrayList<>();
        logs.add(log);
        final CountDownLatch waitLatch = new CountDownLatch(1);
        StoreContext<T> storeContext = new StoreContext(null, null, logs, new ProcessedCallback() {
            @Override
            public void onProcessed(StoreContext context) throws JMQException {
                waitLatch.countDown();
            }
        });
        putJournalLogs(storeContext);
        try {
            waitLatch.wait();
        } catch (InterruptedException e) {}
        return storeContext.getResult();
    }

    @Override
    public RByteBuffer getMessage(final String topic, final int queueId, final long queueOffset) throws
            JMQException {
        checkState();
        // 读取位置信息
        List<QueueItem> qis;
        try {
            qis = queueManager.getQueueItem(topic, queueId, queueOffset, 1);
        } catch (IOException e) {
            throw new JMQException(e.getMessage(), e, JMQCode.SE_IO_ERROR.getCode());
        }
        RByteBuffer refBuf = null;
        try {
            if (qis != null && !qis.isEmpty()) {
                // 读取日志
                QueueItem item = qis.get(0);
                refBuf = journalManager.getJournalMessage(item.getJournalOffset(), item.getSize());

                if (refBuf != null && refBuf.getBuffer() != null) {
                    //读取日志类型
                    short magicCode = Serializer.readMessageMagicCode(refBuf.getBuffer());
                    if (magicCode == BrokerMessage.MAGIC_LOG_CODE) {
                        //读取引用消息
                        BrokerRefMessage refMessage = Serializer.readBrokerRefMessage(Unpooled.wrappedBuffer(refBuf.getBuffer()));
                        //释放资源
                        refBuf.release();
                        if (refMessage != null) {
                            //读取BrokerMessage
                            refBuf = journalManager.getJournalMessage(refMessage.getRefJournalOffset(), refMessage.getRefJournalSize());
                            //重置队列位置
                            if (refBuf != null && refBuf.getBuffer() != null) {
                                //如果是引用消息，将BrokerMessage日志偏移、队列ID和队列偏移重置为引用消息
                                long sendTime = Serializer.readMessageSendTime(refBuf.getBuffer());
                                Serializer.resetMessageLocationAndStoreTime(refBuf.getBuffer(), refMessage.getJournalOffset(),
                                        (byte) refMessage.getQueueId(), refMessage.getQueueOffset(), (int) (refMessage.getStoreTime() - sendTime > 0 ? refMessage.getStoreTime() - sendTime : 0));
                            }
                        }
                    }
                }
            }
        } catch (Throwable e) {
            // 出错释放资源
            if (refBuf != null) {
                refBuf.release();
            }
            if (e instanceof JMQException) {
                throw (JMQException) e;
            }
            throw new JMQException(e.getMessage(), e, JMQCode.SE_IO_ERROR.getCode());
        }

        return refBuf;
    }

    /**
     * 获取索引信息
     *
     * @param topic       主题
     * @param queueId     队列
     * @param queueOffset 队列位置
     * @return
     * @throws JMQException
     */
    @Override
    public QueueItem getQueueItem(final String topic, final int queueId, final long queueOffset) throws JMQException {
        List<QueueItem> qis;
        try {
            qis = queueManager.getQueueItem(topic, queueId, queueOffset, 1);
            if (qis != null && !qis.isEmpty()) {
                return qis.get(0);
            }
        } catch (IOException e) {
            throw new JMQException(e.getMessage(), e, JMQCode.SE_IO_ERROR.getCode());
        }
        return null;
    }

    @Override
    public GetResult getMessage(final String topic, final int queueId, final long queueOffset,
                                final int count, final long maxFrameSize) throws JMQException {
        return getMessage(topic, queueId, queueOffset, count, maxFrameSize, 0);
    }


    @Override
    public GetResult getMessage(final String topic, final int queueId, final long queueOffset,
                                final int count, final long maxFrameSize, int delay) throws JMQException {
        checkState();
        GetResult result = new GetResult();
        List<RByteBuffer> buffers = new ArrayList<RByteBuffer>(count);
        result.setBuffers(buffers);

        // 获取索引数据，会跳过无效的索引数据
        List<QueueItem> qis = skipInvalidOffset(topic, queueId, count, queueOffset);

        if (qis.isEmpty()) {
            return null;
        }
        long ackOffset = qis.get(0).getQueueOffset();

        int i = 0;
        long dataSize = 0;
        RByteBuffer refBuf = null;
        try {
            // 遍历索引数据
            Set<Long> refJournalOffsets = null;
            for (QueueItem item : qis) {
                long journalOffset = item.getJournalOffset();
                // 读取日志数据
                refBuf = journalManager.getJournalMessage(journalOffset, item.getSize());
                boolean isRefMessage = false;
                boolean isTxMessage = false;
                short refQueueId = -1;
                long refQueueOffset = -1;
                long refJournalOffset = -1;
                long refStoreTime = -1;
                if (refBuf != null && refBuf.getBuffer() != null) {
                    //读取日志类型
                    short magicCode = Serializer.readMessageMagicCode(refBuf.getBuffer());
                    isRefMessage = magicCode == BrokerMessage.MAGIC_LOG_CODE;
                    if (isRefMessage) {
                        byte typeCode = Serializer.readMessageTypeCode(refBuf.getBuffer());
                        isTxMessage = typeCode == BrokerMessage.TYPE_TX_MESSAGE;
                        if (!isTxMessage) {
                            //读取引用消息
                            BrokerRefMessage refMessage = Serializer.readBrokerRefMessage(Unpooled.wrappedBuffer(refBuf.getBuffer()));
                            journalOffset = refMessage.getRefJournalOffset();

                            refStoreTime = refMessage.getStoreTime();
                            refQueueId = refMessage.getQueueId();
                            refQueueOffset = refMessage.getQueueOffset();
                            refJournalOffset = journalOffset;

                            //释放资源
                            refBuf.release();
                            //读取BrokerPreMessage
                            refBuf = journalManager.getJournalMessage(journalOffset, refMessage.getRefJournalSize());

                            if (refJournalOffsets == null) {
                                refJournalOffsets = new HashSet<Long>();
                            }
                            //判断是否存在重复消息，如果存在，则需要复制一份
                            if (refBuf != null && refJournalOffsets.contains(refJournalOffset)) {
                                //复制一下当前的buffer
                                ByteBuffer original = refBuf.getBuffer();
                                if (original != null) {
                                    ByteBuffer dest = ByteBuffer.allocate(original.capacity());
                                    original.rewind(); //从头开始拷贝
                                    dest.put(original);
                                    original.rewind();
                                    dest.flip();
                                    // 释放资源
                                    refBuf.release();
                                    // 创建新的引用buffer
                                    refBuf = new RByteBuffer(dest, null);
                                }
                            }
                            refJournalOffsets.add(refJournalOffset);
                        }
                    }
                }

                if (refBuf == null || refBuf.getBuffer() == null) {
                    // 日志数据不存在，判断位置是否越界
                    long minOffset = journalManager.getMinOffset();
                    long maxOffset = journalManager.getMaxOffset();
                    if ((journalOffset < minOffset || journalOffset >= maxOffset)) {
                        if (journalOffset < minOffset) {
                            ackOffset = item.getQueueOffset();
                        }
                        // 越界则查找下一条
                        logger.warn(item + " is not in[" + minOffset + "-" + maxOffset + ") ");
                        // 释放资源
                        if (refBuf != null) {
                            refBuf.release();
                        }
                        continue;
                    }
                    // 没有越界则数据文件读取错误
                    throw new JMQException("getMessage error:" + item, JMQCode.SE_IO_ERROR.getCode());
                } else if (isRefMessage) {
                    //如果是引用消息，将BrokerPreMessage日志偏移、队列ID和队列偏移重置为引用消息
                    if (!isTxMessage) {
                        long sendTime = Serializer.readMessageSendTime(refBuf.getBuffer());
                        Serializer.resetMessageLocationAndStoreTime(refBuf.getBuffer(), refJournalOffset, (byte)
                                refQueueId, refQueueOffset, (int) (refStoreTime - sendTime > 0 ? refStoreTime - sendTime : 0));
                    }
                }


                if (delay > 0) {
                    ByteBuffer buffer = refBuf.getBuffer();
                    long storeTime = Serializer.readMessageStoreTime(buffer);
                    result.setTimeOffset(new MinTimeOffset(item.getQueueOffset(), storeTime));

                    if (storeTime + delay > SystemClock.getInstance().now()) {
                        break;

                    }
                }

                // 累加数据大小
                dataSize += refBuf.getBuffer().remaining();
                if (maxFrameSize > 0 && dataSize > maxFrameSize) {
                    // 超过最大数据包限制，正常情况第一条不应该会超出,如果是第一条就超过由外面网络去检查
                    if (i > 0) {
                        refBuf.release();
                        break;
                    } else {
                        logger.warn(String.format("the message dataSize %d resource maxFrameSize %d", dataSize,
                                maxFrameSize));
                    }
                }
                buffers.add(refBuf);
                // 设置最小索引位置
                if (i == 0) {
                    result.setMinQueueOffset(item.getQueueOffset());
                }
                // 设置最大索引位置
                result.setMaxQueueOffset(item.getQueueOffset());
                i++;
            }


            // 有数据，获取下一条位置
            if (buffers != null && !buffers.isEmpty()) {
                result.setNextOffset(getNextOffset(topic, queueId, result.getMaxQueueOffset()));
            }

            result.setLength(buffers == null ? 0 : buffers.size());
            result.setCode(JMQCode.SUCCESS);
            result.setAckOffset(ackOffset);

        } catch (Throwable e) {
            // 出错释放资源
            if (refBuf != null) {
                refBuf.release();
            }
            // 出错释放资源
            result.release();
            if (e instanceof JMQException) {
                throw (JMQException) e;
            }
            throw new JMQException(e.getMessage(), e, JMQCode.SE_IO_ERROR.getCode());
        }

        return result;
    }

    /**
     * 跳过无效的位置
     *
     * @param topic       主题
     * @param queueId     队列ID
     * @param count       获取数据的条数
     * @param startOffset 起始位置
     * @return
     * @throws JMQException
     */
    protected List<QueueItem> skipInvalidOffset(final String topic, final int queueId, final int count,
                                                long startOffset) throws JMQException {
        List<QueueItem> qis;
        try {
            // 从该位置获取索引数据
            qis = queueManager.getQueueItem(topic, queueId, startOffset, count);
            if (qis == null || qis.isEmpty()) {
                // 没用找到索引数据，判断起始位置过小
                ConsumeQueue cq = queueManager.getConsumeQueue(topic, queueId);
                if (cq != null) {
                    if (startOffset < cq.getMinOffset()) {
                        logger.warn(String.format("topic %s,queueId:%d,minOffset:%d,skip offset:%d", topic, queueId,
                                cq.getMinOffset(), startOffset));
                        startOffset = cq.getMinOffset();
                        qis = queueManager.getQueueItem(topic, queueId, startOffset, count);
                    }
                }

            }

            // 跳过无效的日志位置
            boolean isContinue;
            do {
                isContinue = false;
                if (qis == null || qis.isEmpty()) {
                    break;
                }
                // 取得最后一条索引数据
                QueueItem last = qis.get(qis.size() - 1);
                // 判断日志位置是否无效
                if (last.getJournalOffset() < journalManager.getMinOffset()) {
                    // autoAckForNoJournal(topic, queueId, qis);
                    logger.warn(last + ", journal is not exists, journal minOffset:" + journalManager.getMinOffset());
                    long preOffset = last.getQueueOffset();
                    startOffset = queueManager.getNextOffset(topic, queueId, preOffset);
                    if (startOffset > preOffset) {
                        qis = queueManager.getQueueItem(topic, queueId, startOffset, count);
                        isContinue = true;
                    }
                }
            } while (isContinue);

        } catch (IOException e) {
            throw new JMQException(e.getMessage(), e, JMQCode.SE_IO_ERROR.getCode());
        }
        return qis;
    }

    @Override
    public ConcurrentMap<String, List<Integer>> getQueues() {

        ConcurrentMap<String, List<Integer>> queues = new ConcurrentHashMap<String, List<Integer>>();
        for (ConcurrentMap.Entry<String, ConcurrentMap<Integer, ConsumeQueue>> entry : queueManager.getQueues().entrySet
                ()) {
            Set<Integer> queue = entry.getValue().keySet();
            queues.put(entry.getKey(), Arrays.asList(queue.toArray(new Integer[queue.size()])));
        }

        return queues;
    }

    @Override
    public List<Integer> getQueues(String topic) {
        ConcurrentMap<String, List<Integer>> queues = getQueues();
        return queues.get(topic);
    }

    @Override
    public List<Integer> updateQueues(String topic, int queues) throws JMQException {
        if (!isStarted() || !isReady()) {
            throw new JMQException("store is stopped.", JMQCode.CN_SERVICE_NOT_AVAILABLE.getCode());
        }
        try {
           return queueManager.addTopic(topic, queues);
        } catch (JMQException e) {
            throw e;
        } catch (Exception e) {
            throw new JMQException(e.getMessage(), e, JMQCode.SE_IO_ERROR.getCode());
        }
    }

    public void addQueue(String topic, short queueId) throws JMQException {
        try {
            queueManager.getAndCreateQueue(topic, queueId);
        } catch (Exception e) {
            throw new JMQException(e.getMessage(), e, JMQCode.SE_IO_ERROR.getCode());
        }
    }

    private BrokerMessage getBrokerMessage(String topic, int queue, long offset) throws JMQException {
        BrokerMessage message = null;
        RByteBuffer referenceBuffer = null;
        try {
            referenceBuffer = getMessage(topic, queue, offset);
            if (referenceBuffer == null) {
                return message;
            }
            message = Serializer.readBrokerMessage(Unpooled.wrappedBuffer(referenceBuffer.getBuffer()));
        } catch (Throwable e) {
            if (e instanceof JMQException) {
                throw (JMQException) e;
            }
            throw new JMQException(e.getMessage(), e, JMQCode.SE_IO_ERROR.getCode());
        } finally {
            if (referenceBuffer != null) {
                referenceBuffer.release();
            }
        }
        return message;
    }

    @Override
    public long getMaxOffset() {
        return journalManager.getMaxOffset();
    }

    @Override
    public long getMinOffset() {
        return journalManager.getMinOffset();
    }

    /**
     * 设置复制
     *
     * @param replication 复制
     */
    public void setReplication(Replication replication) {
        this.replication = replication;
    }

    public void addListener(StoreListener listener) {
        eventManager.addListener(listener);
    }

    public void removeListener(StoreListener listener) {
        eventManager.removeListener(listener);
    }

    @Override
    public boolean isDiskFull() {
        if (!isReady()) {
            return false;
        }
        return journalManager.isDiskFull();
    }

    @Override
    public FileStat getDiskFileStat() {
        if (!isReady()) {
            return new FileStat();
        }
        return journalManager.getDiskFileStat();
    }

    @Override
    public StoreConfig getConfig() {
        return this.config;
    }

    public JournalManager getJournalManager() {
        return journalManager;
    }

    public CleanupService getCleanupService() {
        return cleanupService;
    }

    public EventBus<StoreEvent> getEventManager() {
        return eventManager;
    }

    /**
     * 清理所有主题和队列
     */
    public void cleanupAll() throws IOException {
        cleanupService.cleanupAll();
    }

    /**
     * 重置检查点位置
     *
     * @param offset 日志位置
     */
    public void resetCheckPoint(long offset) {
        checkPoint.setJournalOffset(offset);
        checkPoint.setRecoverOffset(offset);
    }


    /**
     * 检查状态
     */
    protected void checkState() {
        if (!isStarted() || !isReady()) {
            throw new IllegalStateException(JMQCode.CN_SERVICE_NOT_AVAILABLE.getMessage());
        }
    }

    /**
     * 是否有数据坏块
     *
     * @return
     */
    @Override
    public boolean hasBadBlock() {
        return journalManager.hasBadBlock();
    }

    @Override
    public long getMinOffsetTimestamp(String topic, int queueId) throws JMQException {
        long minOffset;
        long minOffsetIdTimeStamp;
        minOffset = this.getMinOffset(topic, queueId);
        minOffsetIdTimeStamp = getTimestampByOffset(topic, queueId, minOffset);
        return minOffsetIdTimeStamp;
    }

    @Override
    public long getMaxOffsetTimestamp(String topic, int queueId) throws JMQException {
        long maxOffset;
        long maxOffsetIdTimeStamp;
        maxOffset = this.getMaxOffset(topic, queueId);
        if (maxOffset > 0) {
            maxOffset -= ConsumeQueue.CQ_RECORD_SIZE;
        }
        maxOffsetIdTimeStamp = getTimestampByOffset(topic, queueId, maxOffset);
        return maxOffsetIdTimeStamp;
    }

    @Override
    public PutResult putMessages(List<BrokerMessage> messages, ProcessedCallback callback) throws JMQException {
        if (messages == null || messages.isEmpty()) {
            return null;
        }
        if (!isStarted() || !isReady()) {
            throw new JMQException("store is stopped.", JMQCode.CN_SERVICE_NOT_AVAILABLE.getCode());
        }

        PutResult result = new PutResult();
        result.setCode(JMQCode.CN_UNKNOWN_ERROR);
        try {
//            journalManager.appendJournalLog(messages, result);
            if (result.getCode() == JMQCode.SUCCESS) {
                List<MessageLocation> messageLocations = new ArrayList<MessageLocation>();
                for (BrokerMessage message : messages) {
                    messageLocations.add(new MessageLocation(message.getTopic(), message.getQueueId(), message.getQueueOffset()));
                }
                result.setLocation(messageLocations);
            }
            return result;
        } catch (Exception e) {

            Message message = messages.get(0);
            if (messages.size() == 1) {
                // 单条存储出错
                logger.error(String.format("put message error. topic:%s,app:%s,businessId:%s", message.getTopic(),
                        message.getApp(), message.getBusinessId()), e);
            } else {
                // 批量存储出错
                logger.error(String.format("put batch messages error. topic:%s,app:%s", message.getTopic(), message.getApp()), e);
            }
            if (e instanceof JMQException) {
                throw (JMQException) e;
            }
            throw new JMQException(JMQCode.SE_IO_ERROR, e);
        }
    }

    private long getTimestampByOffset(String topic, int queueId, long offset) throws JMQException {
        checkState();
        long offsetTimestamp = -1L;
        BrokerMessage message = this.getBrokerMessage(topic, queueId, offset);
        if (message != null) {
            offsetTimestamp = message.getStoreTime();
        }
        return offsetTimestamp;
    }

    public QueueItem getMinQueueItem(String topic, int queueId, long minOffset, long maxOffset) throws
            JMQException {
        try {

            if (minOffset >= maxOffset) {
                List<QueueItem> items = null;
                items = queueManager.getQueueItem(topic, queueId, minOffset, 1);

                if (items == null || items.isEmpty()) {
                    logger.error("--invalid offsetvalue--null QueueItem--" + minOffset + "--minoffset--" + this.getMinOffset(topic, queueId));
                    return null;
                }

                if (items.get(0).getJournalOffset() < journalManager.getMinOffset()) {
                    return null;
                } else {
                    return items.get(0);
                }
            }

            /**
             * 找第一个存在与journalFile中的返回
             */
            for (long start = minOffset; start <= maxOffset; start += ConsumeQueue.CQ_RECORD_SIZE) {
                List<QueueItem> items = queueManager.getQueueItem(topic, queueId, minOffset, 1);
                if (!items.isEmpty() && items.get(0).getJournalOffset() >= journalManager.getMinOffset()) {
                    return items.get(0);
                }

            }

            return null;
        } catch (IOException e) {
            throw new JMQException(e.getMessage(), e, JMQCode.SE_IO_ERROR.getCode());
        }
    }

    @Override
    public long getQueueOffsetByTimestamp(String topic, int queueId, String consumer, long resetAckTimeStamp, long
            nextAckOffset) throws JMQException {
        checkState();
        long offsetValue = findConsumerResetAckOffsetByTimestamp(topic, queueId, consumer, resetAckTimeStamp, nextAckOffset);
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("topic:%s,queueId:%d,timestamp:%d,offset:%d", topic, queueId, resetAckTimeStamp, offsetValue));
        }
        return offsetValue;
    }

    @Override
    public Set<Long> getQueueOffsetsBefore(String topic, int queueId, long timestamp, int maxCount) throws
            JMQException {
        checkState();
        Set<Long> offsets = fetchOffsetsBeforeTimestamp(topic, queueId, timestamp, maxCount);
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("topic:%s,queueId:%d,timestamp:%d,offsetsSize:%d", topic, queueId, timestamp, offsets.size()));
        }
        return offsets;
    }

    public GetTxResult getInflightTransaction(String topic, String owner) throws JMQException {
        GetTxResult result = null;
        BrokerPrepare prepare = journalManager.lockAndGetTimeoutTransaction(topic, owner);
        if (prepare != null) {
            result = new GetTxResult();
            result.setTxId(prepare.getTxId());
            result.setTopic(prepare.getTopic());
            result.setQueryId(prepare.getQueryId());
            result.setStoreTime(prepare.getStoreTime());
            List<BrokerRefMessage> messages = prepare.getMessages();
            int count = 0;
            if (messages != null && !messages.isEmpty()) {
                count = messages.size();
                List<RByteBuffer> buffers = new ArrayList<RByteBuffer>(count);
                try {
                    for (BrokerRefMessage message : messages) {
                        RByteBuffer refBuf = journalManager.getJournalMessage(message.getRefJournalOffset(), message.getRefJournalSize());
                        buffers.add(refBuf);
                    }
                } catch (Exception e) {
                    throw new JMQException("Get BrokerPreMessage error.", e, JMQCode.SE_IO_ERROR.getCode());
                }
            }
            result.setMsgCount(count);
        }
        return result;
    }

    @Override
    public RByteBuffer readMessageBlock(long offset, int size) throws Exception {
        //maxOffset - offset maybe resource Integer.maxValue
        size = (int) Math.min(getMaxOffset() - offset, size);
        try {
            return journalManager.getJournalMessage(offset, size);
        } catch (JMQException e) {
            throw e;
        } catch (Exception e) {
            throw new JMQException(e.getMessage(), e, JMQCode.SE_IO_ERROR.getCode());
        }
    }

    @Override
    public void writeMessageBlock(long offset, RByteBuffer buffer, boolean buildIndex) throws Exception {
        if (buffer == null || buffer.getBuffer() == null || offset < 0) {
            return;
        }
        try {
            // 获取文件
            AppendFile af = journalManager.getJournalDir().getAndCreatePrev(offset, true);
            if (af == null) {
                throw new JMQException(JMQCode.SE_INVALID_OFFSET, offset);
            }
            // 数据块大小
            int size = buffer.remaining();
            // 复制位置校验
            if (offset > getMaxOffset()) {
                logger.error(String.format("cannot write offset exceeded store max offset: offset:%d, maxOffset:%d", offset, getMaxOffset()));
                throw new JMQException(JMQCode.SE_INVALID_OFFSET, offset);
            }
            // 写入文件
            if (size <= af.remaining()) {
                af.write(buffer.getBuffer(), (int) (offset - af.id()));
            } else {
                throw new JMQException(JMQCode.SE_MESSAGE_SIZE_EXCEEDED);
            }
            // 异步建立队列索引
            if (buildIndex) {
                redispatchService.setMaxJournalOffset(journalManager.getMaxOffset());
            }
            if (config.getJournalFlushPolicy().isSync()) {
                // 刷盘，组提交
                FlushRequest request = new FlushRequest();
                request.getWrite().begin();
                request.setOffset(offset);
                request.setSize(size);
                journalManager.getCommitManager().add(request);
            }
        } catch (JMQException e) {
            throw e;
        } catch (Exception e) {
            throw new JMQException(JMQCode.SE_IO_ERROR, e);
        }
    }

    @Override
    public void cleanUp(Map<String, Long> minAckOffsets) throws Exception {
        cleanupService.cleanup(minAckOffsets);
    }

    @Override
    public BrokerPrepare getBrokerPrepare(String txId) {
        return journalManager.getBrokerPrepare(txId);
    }

    public void unLockInflightTransaction(String txId) {
        journalManager.unLockInflightTransaction(txId);
    }

    @Override
    public void onTransactionFeedback(String txId) {
        journalManager.onTransactionFeedback(txId);
    }

    @Override
    public int getRestartSuccess() {
        return restartSucces;
    }

    @Override
    public int getRestartTimes() {
        return restartTimes;
    }

    /**
     * 查找指定时间戳之前的索引文件IDs
     *
     * @param topic
     * @param queueId
     * @param timestamp
     * @param maxNumOffsets
     * @return
     * @throws JMQException
     */
    private Set<Long> fetchOffsetsBeforeTimestamp(String topic, int queueId, long timestamp, int maxNumOffsets) throws
            JMQException {
        Set<Long> offsets = new HashSet<Long>();
        ConsumeQueue consumeQueue;
        List<AppendFile> appendFiles;

        consumeQueue = queueManager.getConsumeQueue(topic, queueId);
        if (consumeQueue == null) {
            return offsets;
        }
        appendFiles = consumeQueue.getAppendDirectory().files();
        if (appendFiles == null || appendFiles.isEmpty()) {
            if (timestamp == LATEST_TIME || timestamp == EARLIEST_TIME) {
                offsets.add(0L);
            }
            return offsets;
        }
        AppendFile maxAppendFile;
        AppendFile minAppendFile;
        // appendFiles是有序的
        maxAppendFile = appendFiles.get(appendFiles.size() - 1);
        minAppendFile = appendFiles.get(0);
        int numOffsets = 0;
        for (AppendFile appendFile : appendFiles) {
            if (timestamp == LATEST_TIME) {
                break;
            } else if (timestamp == EARLIEST_TIME) {
                break;
            } else {
                long lastModified = appendFile.channelFile().getFile().lastModified();
                if (lastModified <= timestamp) {
                    long baseOffset = appendFile.id();
                    numOffsets++;
                    if (numOffsets <= maxNumOffsets) {
                        offsets.add(baseOffset);
                    } else {
                        break;
                    }
                }
                long baseOffset = appendFile.header().getId();
                offsets.add(baseOffset);
            }
        }

        if (timestamp == LATEST_TIME) {
            long maxId = maxAppendFile.id();
            long latestBaseOffset = maxId + maxAppendFile.flushPosition();
            offsets.add(latestBaseOffset);
        } else if (timestamp == EARLIEST_TIME) {
            offsets.add(minAppendFile.id());
        }

        return offsets;
    }

    private long findConsumerResetAckOffsetByTimestamp(String topic, int queueId, String consumer, long
            resetAckTimeStamp, long nextAckOffset) throws JMQException {
        long mRetOffset = -1L;
        ConsumeQueue consumeQueue;
        List<AppendFile> appendFiles;
        AppendFile appendFile;

        consumeQueue = queueManager.getConsumeQueue(topic, queueId);
        if (consumeQueue == null) {
            return mRetOffset;
        }
        appendFiles = consumeQueue.getAppendDirectory().files();
        if (appendFiles == null || appendFiles.isEmpty()) {
            return mRetOffset;
        }
        //遍历队列目录下的队列文件，查找进行确认消费位置的队列文件
        for (int index = appendFiles.size() - 1; index >= 0; index--) {
            appendFile = appendFiles.get(index);
            if (appendFile.header().getCreateTime() <= resetAckTimeStamp) {
                long offsetId = appendFile.id();
                mRetOffset = offsetId;
                if (appendFile.header().getCreateTime() == resetAckTimeStamp) {
                    mRetOffset = offsetId;
                    return mRetOffset;
                }
                mRetOffset = binarySearchForOffsetInAppendFile(topic, queueId, consumer, resetAckTimeStamp,
                        nextAckOffset, appendFile);
                break;
            }
        }
        return mRetOffset;
    }

    //根据二分查找找出索引
    private long binarySearchForOffsetInAppendFile(String topic, int queueId, String consumer, long time, long
            nextAckOffset, AppendFile
                                                           appendFile) throws JMQException {
        long searchCount = 0;
        long mRetOffset = -1L;
        long offsetId = appendFile.id();
        // 如果是第一个索引文件offset等于0
        if (offsetId < 0) {
            return mRetOffset;
        } else if (offsetId > 0) {
            //至少可以确认到该文件之前的消息了，要做小于0的判断
            mRetOffset = offsetId - ConsumeQueue.CQ_RECORD_SIZE;
        }
        long startOffset = offsetId;
        long endOffset = startOffset + appendFile.flushPosition() - ConsumeQueue.CQ_RECORD_SIZE;
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("topic:%s,queueId:%d,startOffset:%d,endOffset:%d,nextAckOffset:%d,app:%s",
                    topic, queueId, startOffset, endOffset, nextAckOffset, consumer));
        }
        if (endOffset <= nextAckOffset) {
            mRetOffset = nextAckOffset;
            if (mRetOffset == endOffset) {
                mRetOffset += ConsumeQueue.CQ_RECORD_SIZE;
            }
            return mRetOffset;
        }
        long checkOffset;
        if ((startOffset < nextAckOffset) && (nextAckOffset % 22 == 0)) {
            startOffset = nextAckOffset;
        }
        long preEndOffset = endOffset;
        while (endOffset >= startOffset) {
            searchCount++;
            long count = (endOffset - startOffset) / ConsumeQueue.CQ_RECORD_SIZE;
            long mid = count / 2;
            checkOffset = startOffset + mid * ConsumeQueue.CQ_RECORD_SIZE;
            BrokerMessage message = this.getBrokerMessage(topic, queueId, checkOffset);
            if (message == null || message.getStoreTime() <= 0) {
                //判断失败
                logger.error(String.format("topic:%s,queueId:%d invalid offset:%d", topic, queueId, checkOffset));
                startOffset += ConsumeQueue.CQ_RECORD_SIZE;
                continue;
            }
            if (message.getStoreTime() == time) {
                mRetOffset = checkOffset;
                return mRetOffset;
            } else if (message.getStoreTime() > time) {
                endOffset = checkOffset - ConsumeQueue.CQ_RECORD_SIZE;
            } else {
                mRetOffset = checkOffset;
                startOffset = checkOffset + ConsumeQueue.CQ_RECORD_SIZE;
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace(String.format("searchCount:%d,topic:%s,endOffset:%d,startOffset:%d,queueId:%d,invalid offset:%d", searchCount, topic, endOffset, startOffset, queueId, mRetOffset));
        }
        if (mRetOffset == preEndOffset) {
            mRetOffset += ConsumeQueue.CQ_RECORD_SIZE;
        }
        return mRetOffset;
    }

    public Lock getLock() {
        return super.getWriteLock();
    }

    @Override
    public void updateWaterMark(long waterMark) {
        this.waterMark = waterMark;
    }

    @Override
    public void truncate(long offset) throws IOException {
        journalManager.truncate(offset);
    }

    protected static class PendingMessage implements Comparable<Long> {
        public PendingMessage(Long journalOffset, StoreContext context, ProcessedCallback oldCallback) {
            this.journalOffset = journalOffset;
            this.context = context;
            this.oldCallback = oldCallback;
        }

        @Override
        public int compareTo(Long offset) {
            return journalOffset.compareTo(offset);
        }

        public void done(JMQCode code, long waterMark) throws JMQException {
            context.getResult().setReplicationCode(code);
            if (context.getResult().getCode() == JMQCode.SUCCESS) {
                List<MessageLocation> messageLocations = new ArrayList<MessageLocation>();
                for (BrokerMessage message : context.getLogs()) {
                    messageLocations.add(new MessageLocation(message.getTopic(), message.getQueueId(), message.getQueueOffset()));
                }
                context.getResult().setLocation(messageLocations);
            }
            oldCallback.onProcessed(context);
        }

        public Long journalOffset;
        private StoreContext<BrokerMessage> context;
        private ProcessedCallback oldCallback;
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
            long waterMark = JMQStore.this.waterMark;

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