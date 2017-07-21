package com.ipd.jmq.server.store;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.cluster.ClusterRole;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.exception.JMQJournalException;
import com.ipd.jmq.common.message.*;
import com.ipd.jmq.common.model.JournalLog;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.command.TxPrepare;
import com.ipd.jmq.common.network.v3.netty.NettyDecoder;
import com.ipd.jmq.server.store.buffer.ByteBufPool;
import com.ipd.jmq.server.store.journal.*;
import com.ipd.jmq.toolkit.buffer.RByteBuffer;
import com.ipd.jmq.toolkit.concurrent.EventBus;
import com.ipd.jmq.toolkit.concurrent.RingBuffer;
import com.ipd.jmq.toolkit.io.Files;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.time.SystemClock;
import com.ipd.jmq.toolkit.validate.Validators;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

/**
 * 日志文件处理
 *
 * @author dingjun
 */
public class JournalManager extends Service {
    private static final String JOURNAL_MAPPED_MANAGER = "JMQ_journalMappedFileManager";
    public static final short MAGIC_MSG_CODE = BrokerMessage.MAGIC_CODE;
    public static final short MAGIC_BLANK_CODE = BrokerMessage.MAGIC_BLANK_CODE;
    public static final short MAGIC_LOG_CODE = BrokerMessage.MAGIC_LOG_CODE;
    public static final int MESSAGE_HEAD_LEN = 4 + 2;
    private static Logger logger = LoggerFactory.getLogger(JournalManager.class);
    // 待写入消息缓冲区，单线程顺序写
    private RingBuffer<JournalEvent> flushingMessagesQueue;
    //文件配置
    JournalConfig journalConfig;
    // 存储配置
    private StoreConfig config;
    //broker
    private Broker broker;
    // 追加文件管理器
    private AppendFileManager appendFileManager;
    // 追加数据目录
    private AppendDirectory journalDir;
    // 派发服务
    private DispatchService dispatchService;
    // 组提交服务
    private CommitManager commitManager;
    // 队列管理器
    private QueueManager queueManager;
    // 追加消息刷盘条件
    private FlushPolicy.FlushCondition appendFlush = new FlushPolicy.FlushCondition();
    // 条数
    private AtomicLong count = new AtomicLong(0);
    // 共享数据缓冲区
    private ByteBufPool heapBufferPool;
    // 存储监听器
    private EventBus<StoreEvent> eventManager;
    // 清理管理器
    private CleanupService cleanupService;
    // 正在处理的事务
    protected ConcurrentMap<String, InflightTx> inflights = new ConcurrentHashMap<String, InflightTx>();
    // 事务计数器
    protected ConcurrentMap<String, AtomicInteger> inflightCounter = new ConcurrentHashMap<String, AtomicInteger>();
    protected volatile int flushDiskTimeout = 0;

    protected class CommitManagerEx extends CommitManager {
        public CommitManagerEx(AppendDirectory directory, QueueConfig config, FileStat fileStat) {
            super(directory, config, fileStat);
        }

        @Override
        protected void doStart() throws Exception {
            events = new RingBuffer(config.getSize(), new CommitHandlerEx(), CommitManagerEx.class.getSimpleName());
            events.start();
            logger.info("commit manager is started.");
        }

        @Override
        protected long flush() {
            long flushedJournalOffset = super.flush();
            notifyFlushedJournalOffset(flushedJournalOffset);
            return flushedJournalOffset;
        }

        protected class CommitHandlerEx extends CommitHandler {
            @Override
            public void onEvent(final Object[] elements) throws Exception {
                if (elements != null && elements.length > 0) {
                    super.onEvent(elements);
                } else if ((flushDiskTimeout > 0) && (--flushDiskTimeout == 0)) {
                    flush();
                }
            }

            @Override
            public long getInterval() {
                return 1;
            }
        }
    }

    private void notifyFlushedJournalOffset(long flushedJournalOffset) {
        eventManager.inform(new StoreEvent(flushedJournalOffset));
    }

    /**
     * 构造函数
     *
     * @param config 存储配置
     */
    public JournalManager(StoreConfig config, Broker broker, DispatchService dispatchService, QueueManager queueManager, EventBus<StoreEvent> eventManager) {
        if (config == null) {
            throw new IllegalArgumentException("config can not be null");
        }
        this.config = config;
        this.journalConfig = config.getJournalConfig();
        this.broker = broker;
        this.heapBufferPool = new ByteBufPool(config.getBufferPoolSize(), 1024 * 50);
        this.eventManager = eventManager;
        this.queueManager = queueManager;
        this.dispatchService = dispatchService;
        this.flushingMessagesQueue = new RingBuffer<JournalEvent>(config.getWriteQueueSize(), new FlushingMessagesQueueHandler(), "flushingMessagesQueue");
    }

    @Override
    protected void validate() throws Exception {
        super.validate();
        // 验证配置是否合理
        Validators.validate(journalConfig);
        File directory = config.getJournalDirectory();
        Files.createDirectory(directory);

    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        // 追加目录
        appendFileManager = new AppendFileManager(journalConfig, new VersionHeader(), JOURNAL_MAPPED_MANAGER);
        appendFileManager.start();

        journalDir = appendFileManager.getDirectory(config.getJournalDirectory());

        commitManager = new CommitManagerEx(journalDir, new QueueConfig(config.getCommitQueueSize(), config.getFlushTimeout()), appendFileManager.getFileStat());
        commitManager.start();
        flushingMessagesQueue.start();

        // 获取文件
        journalDir.lastAndCreate();
        appendFlush.reset(SystemClock.getInstance().now());
        logger.info("journal manager is started.");
    }

    @Override
    protected void doStop() {
        Close.close(commitManager).close(appendFileManager);
        logger.info(String.format("journal manager is stopped. append message count:%d", count.get()));
    }

    public void setCleanupService(CleanupService cleanupService) {
        this.cleanupService = cleanupService;
    }

    /**
     * 从倒数第三个文件开始恢复(此处未考虑事务消息)
     *
     * @throws Exception
     */
    protected void recover() throws Exception {
        List<AppendFile> files = journalDir.files();
        if (files == null || files.isEmpty()) {
            return;
        }
        // 从最后第三个文件开始
        int startIndex = files.size() - 3;
        if (startIndex < 0) {
            startIndex = 0;
        }

        AppendFile file = files.get(startIndex);
        recover(file.id());
    }

    /**
     * 从全局的指定位置开始恢复
     * 检测到数据内部有坏数据，需要抛出异常终止，末尾的坏数据可以截掉
     *
     * @param offset 全局偏移量
     * @throws Exception
     */
    public void recover(long offset) throws Exception {
        // 文件按照ID升序排序
        List<AppendFile> files = journalDir.files();
        if (files == null || files.isEmpty()) {
            return;
        }

        // 计算从第几个文件开始进行恢复
        int fileIndex = 0;
        // 倒序遍历所有文件，找到最接近的文件
        for (int i = files.size() - 1; i >= 0; i--) {
            if (offset >= files.get(i).id()) {
                fileIndex = i;
                break;
            }
        }
        AppendFile file = files.get(fileIndex);
        // 判断文件是否丢失
        if (offset < file.id() || offset >= file.id() + file.length()) {
            logger.warn(String.format("journal may be deleted, recover expect %d, real %d", offset, file.id()));
            offset = file.id();
        }

        try {
            logger.info(String.format("recover journal and transactions, file:%s offset:%d", file.file().getName(),
                    offset));
            recover(files, fileIndex, offset);
        } finally {
            // 等待队列恢复完成
            while (isStarted() && dispatchService.hasRemaining()) {
                await(1000);
                logger.info("wait queue dispatch finish ...");
            }
            logger.info(String.format("max journal offset is %d,recover queue dispatch journalOffset is %d",
                    this.getMaxOffset(), dispatchService.getJournalOffset()));
        }
    }

    /**
     * 恢复
     *
     * @param files     文件列表
     * @param fileIndex 开始文件索引
     * @param offset    偏移量
     * @throws Exception
     */
    protected void recover(List<AppendFile> files, int fileIndex, long offset) throws Exception {
        AppendFile file = files.get(fileIndex);

        int filePos = (int) (offset - file.id());
        long fileEnd = file.id() + file.length() - file.headSize();

        RByteBuffer refBuf = null;
        JournalLog journalLog;
        int recovers = 0;
        int blockSize;
        int size = 0;
        short magic = 0;
        int blockPos;
        ByteBuffer buf;

        // 循环恢复数据
        while (isStarted() && fileIndex < files.size()) {
            // 是否需要恢复下一个文件
            if (file == null) {
                // 获取下一个文件
                file = files.get(fileIndex);
                offset = file.id();
                filePos = 0;
                fileEnd = offset + file.length() - file.headSize();
                logger.info(String.format("recover journal file:%s", file.file().getName()));
            }
            // 读取一块数据
            try {
                refBuf = new RByteBuffer(file.read(filePos, config.getBlockSize()), file);
                buf = refBuf.getBuffer();
                blockSize = buf == null ? 0 : buf.remaining();
                if (blockSize < MESSAGE_HEAD_LEN) {
                    if (filePos + blockSize == file.length()) {
                        // 文件结尾，没有数据
                        file = null;
                        offset += blockSize;
                        fileIndex++;
                        continue;
                    } else {
                        throw new JMQJournalException(file.id(), filePos);
                    }
                }
                // 有数据
                blockPos = 0;

                // 循环块数据
                while (isStarted() && blockPos + MESSAGE_HEAD_LEN <= blockSize) {

                    // 读取头部数据
                    size = buf.getInt();
                    magic = buf.getShort();

                    if (size > 0 && size <= NettyDecoder.FRAME_MAX_SIZE && magic == MAGIC_BLANK_CODE) {
                        // 空白数据
                        if (size + offset == fileEnd) {
                            // 文件末尾，有效空白数据，切换到下一个文件
                            fileIndex++;
                            file = null;
                            offset += size;
                            break;
                        }
                        // 空白位置不在文件末尾，则无效
                        throw new JMQJournalException(file.id(), filePos);
                    } else if (size > 0 && size <= NettyDecoder.FRAME_MAX_SIZE && (magic == MAGIC_MSG_CODE || magic == MAGIC_LOG_CODE)) {

                        // 消息数据
                        if (blockPos + size > blockSize) {
                            // 剩余的块数据不够
                            if (offset + size <= fileEnd) {
                                // 该文件还有足够的数据，则从该位置重新读取块数据
                                break;
                            }
                            // 该文件没用足够的数据，则该标示无效
                            throw new JMQJournalException(file.id(), filePos);
                        }
                        // 有足够数据，定位到头部开始
                        buf.position(blockPos);

                        try {
                            //是否需要建索引
                            boolean needDispatch = false;

                            if (magic == MAGIC_MSG_CODE) {
                                // 反序列化 BrokerMessage
                                journalLog = decodeBrokerMessage(buf, size);
                                //BrokerMessage 需要重建索引
                                needDispatch = true;
                            } else {  // 反序列化 Journal
                                // 获取JournalLog类型
                                byte type = buf.get(buf.position() + 4 + 2);

                                ByteBuffer view = buf.slice();
                                view.limit(size);//防止里面有问题，超出长度

                                ByteBuf wrappedBuffer = Unpooled.wrappedBuffer(view);
                                switch (type) {
                                    case JournalLog.TYPE_TX_PREPARE: {
                                        journalLog = Serializer.readBrokerPrepare(wrappedBuffer);
                                        //开启事务
                                        addTransaction((BrokerPrepare) journalLog);
                                        break;
                                    }
                                    case JournalLog.TYPE_TX_COMMIT: {
                                        journalLog = Serializer.readBrokerCommit(wrappedBuffer);
                                        //事务已经提交，移除事务
                                        BrokerCommit commit = (BrokerCommit) journalLog;
                                        if (commit != null) {
                                            removeTransaction(commit.getTxId());
                                        }
                                        break;
                                    }
                                    case JournalLog.TYPE_TX_ROLLBACK: {
                                        journalLog = Serializer.readBrokerRollback(wrappedBuffer);
                                        //事务已经回滚，移除事务
                                        BrokerRollback rollback = (BrokerRollback) journalLog;
                                        if (rollback != null) {
                                            removeTransaction(rollback.getTxId());
                                        }
                                        break;
                                    }
                                    case JournalLog.TYPE_TX_MESSAGE:
                                        journalLog =  decodeBrokerMessage(buf, size);
                                        //事务消息需要重建索引
                                        needDispatch = true;
                                        break;
                                    case JournalLog.TYPE_TX_PRE_MESSAGE: {
                                        journalLog = decodeBrokerMessage(buf, size);
                                        BrokerMessage preMessage = (BrokerMessage) journalLog;
                                        //将事务消息加入到未提交事务
                                        BrokerRefMessage refMessage = createRefMsg(preMessage);
                                        //恢复事务时如果抛出事务不存在异常,可以忽略,因为这种情况的事务已经被提交或回滚
                                        try {
                                            addBrokerRefMessage(preMessage.getTxId(), refMessage);
                                        } catch (JMQException e) {
                                            logger.warn("recover transaction adding broker ref message error, txid is not exist", e);
                                        }
                                        break;
                                    }
                                    case JournalLog.TYPE_REF_MESSAGE: {
                                        journalLog = Serializer.readBrokerRefMessage(wrappedBuffer);

                                        //事务引用消息需要重建索引
                                        needDispatch = true;
                                        break;
                                    }
                                    default:
                                        throw new Exception(String.format("no such log type.type=%s", type));
                                }
                            }

                            if (needDispatch) {
                                // 派发数据
                                redispatch(journalLog);
                            }
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                            throw new JMQJournalException(file.id(), filePos);
                        }
                        recovers++;
                        offset += size;
                        filePos += size;
                        blockPos += size;
                        buf.position(blockPos);
                    } else {
                        // 无效数据
                        throw new JMQJournalException(file.id(), filePos);
                    }

                }
            } catch (JMQJournalException e) {
                // 尝试往后读
                long recoverOffset = findNextMessage(files, fileIndex, offset);
                // 长时间校验，重新判断启动标示
                if (recoverOffset >= 0) {
                    // 后面有数据，备份数据
                    logger.error(
                            String.format("journal data has bad block from %d to %d. backup data and truncate.", offset,
                                    recoverOffset), e);
                    backupBadBlock(offset, files.subList(fileIndex, files.size()));
                    if (broker.getRole() == ClusterRole.MASTER && !config.isIgnoreJournalError()) {
                        throw e;
                    }
                } else if (size == 0 && magic == 0 && recoverOffset == -1) {
                    // 文件末尾
                } else {
                    // 数据有损坏
                    logger.info(e.getMessage());
                }
                break;
            } catch (Exception e) {
                logger.error("", e);
                throw e;
            } finally {
                if (refBuf != null) {
                    refBuf.release();
                    refBuf = null;
                }
            }
        }
        logger.info(String.format("recover message count:%d", recovers));
        // 清理掉垃圾数据
        if (isStarted()) {
            truncate(offset);
        } else {
            logger.warn("service is stopping");
        }
    }

    public void truncate(long offset) throws IOException {
        journalDir.truncate(offset);
    }

    public static BrokerRefMessage createRefMsg(BrokerMessage preMessage) {
        BrokerRefMessage refMessage = new BrokerRefMessage();
        refMessage.setRefJournalOffset(preMessage.getJournalOffset());
        refMessage.setRefJournalSize(preMessage.getSize());
        refMessage.setQueueId(preMessage.getQueueId());
        refMessage.setFlag(preMessage.getFlag());
        refMessage.setTopic(preMessage.getTopic());
        return refMessage;
    }

    /**
     * 解析消息
     *
     * @param buf  缓冲区
     * @param size 数据大小
     * @return 消息对象
     * @throws Exception
     */
    protected BrokerMessage decodeBrokerMessage(final ByteBuffer buf, final int size) throws Exception {
        BrokerMessage message;
        long checksum;
        ByteBuffer view = buf.slice();
        view.limit(size);//防止里面有问题，超出长度
        // 反序列化
        message = Serializer.readBrokerMessage(Unpooled.wrappedBuffer(view));
        if (config.isChecksumOnRecover()) {
            // 验证CRC
            checksum = message.getBodyCRC();
            message.setBodyCRC(0);
            if (checksum != message.getBodyCRC() || message.getTopic() == null || message.getTopic().isEmpty()) {
                throw new Exception("crc is invalid");
            }
        }
        return message;
    }

    /**
     * 备份坏块文件
     *
     * @param offset 偏移量
     * @param files  文件
     * @throws IOException
     */
    protected void backupBadBlock(long offset, List<AppendFile> files) throws IOException {
        long timestamp = SystemClock.now();
        File parent = new File(config.getBadFileDirectory(), String.format("%d-%d%s", timestamp, offset, "/"));
        Files.createDirectory(parent);

        File target;
        for (AppendFile file : files) {
            if (isStarted()) {
                target = new File(parent, file.file().getName());
                if (target.exists()) {
                    target.delete();
                }
                Files.createFile(target);
                Files.copy(file.file(), target);
            }
        }
    }

    /**
     * 是否有数据坏块
     *
     * @return 有数据坏块标示
     */
    public boolean hasBadBlock() {
        File directory = config.getBadFileDirectory();
        if (directory.exists()) {
            File[] children = directory.listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    return pathname.isDirectory();
                }
            });

            if (children != null && children.length > 0) {
                return true;
            }

        }
        return false;

    }


    /**
     * 往后查找直到有效消息为止
     *
     * @param files     文件列表
     * @param fileIndex 文件索引
     * @param offset    全局开始位置
     * @return 有效消息位置
     * <li>=-1 没有找到位置</li>
     * <li>&gt;=0 消息位置</li>
     * @throws IOException
     */
    protected long findNextMessage(List<AppendFile> files, int fileIndex, long offset) throws IOException {

        long END = -1;

        AppendFile file = files.get(fileIndex);
        int filePos = (int) (offset - file.id());
        long fileEnd = file.id() + file.length() - file.headSize();

        RByteBuffer refBuf = null;
        int blockSize;
        int blockPos;
        int size;
        short magic;
        BrokerMessage message = null;
        long checksum;
        ByteBuffer buf;
        ByteBuffer view;

        // 循环文件
        while (isStarted() && fileIndex < files.size()) {
            // 是否需要恢复下一个文件
            if (file == null) {
                // 获取下一个文件
                file = files.get(fileIndex);
                offset = file.id();
                fileEnd = offset + file.length() - file.headSize();
                filePos = 0;
            }

            // 读取一块数据
            try {
                if(filePos < file.maxDataSize())  {
                    refBuf = new RByteBuffer(file.read(filePos, config.getBlockSize()), file);
                    buf = refBuf.getBuffer();
                } else {
                    buf = null;
                }

                if (buf == null || buf.remaining() < MESSAGE_HEAD_LEN) {
                    // 文件尾部
                    fileIndex++;
                    file = null;
                    continue;
                }
                blockSize = buf.remaining();
                blockPos = 0;
                // 循环块数据
                while (isStarted() && blockPos + MESSAGE_HEAD_LEN <= blockSize) {
                    // 读取头部数据
                    size = buf.getInt();
                    magic = buf.getShort();
                    if (size > 0 && size <= NettyDecoder.FRAME_MAX_SIZE && magic == MAGIC_BLANK_CODE) {
                        // 空白数据
                        if (size + offset == fileEnd) {
                            // 文件末尾，有效空白数据，切换到下一个文件
                            fileIndex++;
                            file = null;
                            break;
                        }
                    } else if (size > 0 && size <= NettyDecoder.FRAME_MAX_SIZE && (magic == MAGIC_MSG_CODE || magic == MAGIC_LOG_CODE)) {
                        // 消息数据
                        if (blockPos + size > blockSize) {
                            // 剩余的块数据不够
                            if (offset + size <= fileEnd) {
                                // 该文件还有足够的数据，则从该位置重新读取块数据
                                break;
                            }
                            // 该文件没用足够的数据，则该标示可能无效
                        } else {
                            // 有足够数据，定位到头部开始
                            buf.position(blockPos);
                            view = buf.slice();
                            view.limit(size);
                            buf.position(blockPos + size);
                            try {
                                // 反序列化消息
                                if (magic == MAGIC_MSG_CODE) {
                                    message = Serializer.readBrokerMessage(Unpooled.wrappedBuffer(view));
                                } else {
                                    byte type = view.get();
                                    switch (type) {
                                        case JournalLog.TYPE_TX_PREPARE: {
                                            Serializer.readBrokerMessage(Unpooled.wrappedBuffer(view));
                                            break;
                                        }
                                        case JournalLog.TYPE_TX_COMMIT: {
                                            Serializer.readBrokerCommit(Unpooled.wrappedBuffer(view));
                                            break;
                                        }
                                        case JournalLog.TYPE_TX_ROLLBACK: {
                                            Serializer.readBrokerRollback(Unpooled.wrappedBuffer(view));
                                            break;
                                        }
                                        case JournalLog.TYPE_TX_PRE_MESSAGE: {
                                            message = Serializer.readBrokerMessage(Unpooled.wrappedBuffer(view));
                                            break;
                                        }
                                        case JournalLog.TYPE_REF_MESSAGE: {
                                            Serializer.readBrokerRefMessage(Unpooled.wrappedBuffer(view));
                                            break;
                                        }
                                    }
                                }

                                if (message != null && config.isChecksumOnRecover()) {
                                    // 验证CRC
                                    checksum = message.getBodyCRC();
                                    message.setBodyCRC(0);
                                    if (checksum == message.getBodyCRC()) {
                                        // 有效数据
                                        return offset;
                                    }
                                } else {
                                    // 有效数据
                                    return offset;
                                }
                            } catch (Exception ignored) {
                                // 反序列化错误
                            }
                        }
                    }

                    if (size == 0 && magic == 0) {
                        blockPos += MESSAGE_HEAD_LEN;
                        filePos += MESSAGE_HEAD_LEN;
                        offset += MESSAGE_HEAD_LEN;
                        buf.position(blockPos);
                    } else {
                        // 定位到下一个字节
                        blockPos++;
                        filePos++;
                        offset++;
                        buf.position(blockPos);
                    }
                }
            } finally {
                if (refBuf != null) {
                    refBuf.release();
                    refBuf = null;
                }
            }
        }
        return END;
    }

    /**
     * 获取统计信息
     *
     * @return 统计信息
     */
    public FileStat getFileStat() {
        return appendFileManager.getFileStat();
    }

    /**
     * 重新建立队列数据
     *
     * @param message 消息
     * @return 派发请求
     * @throws Exception
     */
    public DispatchRequest redispatch(final BrokerMessage message) throws Exception {
        if (message == null) {
            return null;
        }
        if (!isStarted()) {
            throw new JMQException(JMQCode.CN_SERVICE_NOT_AVAILABLE);
        }
        if (isDiskFull()) {
            cleanupService.forceCleanup();
            if (isDiskFull()) {
                throw new JMQException(JMQCode.SE_DISK_FULL);
            }
        }
        DispatchRequest request = new DispatchRequest(message, true, false);
        ConsumeQueue queue = queueManager.getAndCreateQueue(message.getTopic(), message.getQueueId());
        if (queue.getOffset() <= message.getQueueOffset()) {
            queue.setOffset(message.getQueueOffset());
            queue.updateOffset();
        }
        //queue.append(request);
        dispatchService.dispatch(request);
        return request;
    }

    public DispatchRequest redispatch(final JournalLog message) throws Exception {
        if (message == null) {
            return null;
        }
        if (!isStarted()) {
            throw new JMQException(JMQCode.CN_SERVICE_NOT_AVAILABLE);
        }
        if (isDiskFull()) {
            cleanupService.forceCleanup();
            if (isDiskFull()) {
                throw new JMQException(JMQCode.SE_DISK_FULL);
            }
        }

        short queueId = -1;
        long queueOffset = -1;
        String topic = null;
        DispatchRequest request = null;

        if (message instanceof BrokerRefMessage) {
            BrokerRefMessage refMessage = (BrokerRefMessage) message;
            topic = refMessage.getTopic();
            queueId = refMessage.getQueueId();
            queueOffset = refMessage.getQueueOffset();
            //构造派发请求
            request = new DispatchRequest(refMessage, message.getSize(), true, false);
        } else if (message instanceof BrokerMessage) {
            BrokerMessage brokerMessage = (BrokerMessage) message;
            topic = brokerMessage.getTopic();
            queueId = brokerMessage.getQueueId();
            queueOffset = brokerMessage.getQueueOffset();
            //构造派发请求
            request = new DispatchRequest(brokerMessage, true, false);
        }

        if (request == null || topic == null || queueId == -1 || queueOffset == -1) {
            throw new Exception(String.format("Redispatch message error,request=%s,topic=%s,queueId=%s," +
                    "queueOffset=%s", request, topic, queueId, queueOffset));
        }

        ConsumeQueue queue = queueManager.getAndCreateQueue(topic, queueId);
        if (queue.getOffset() <= queueOffset) {
            queue.setOffset(queueOffset);
            queue.updateOffset();
        }
        dispatchService.dispatch(request);
        return request;
    }

    protected void appendJournalLog(StoreContext<?> context) throws Exception {
        if (context == null) {
            return;
        }
        if (!isStarted()) {
            throw new JMQException(JMQCode.CN_SERVICE_NOT_AVAILABLE);
        }
        boolean stat = false;
        long v = count.get();
        if (v % 100 == 0) {
            stat = true;
        }
        long storeTime = SystemClock.now();
        // 设置存储时间
        PutResult result = context.getResult();
        result.setStoreTime(storeTime);
        List<ByteBuf> buffers = new ArrayList<ByteBuf>();
        int logSize = 0;
        // needSync = true 表示处理需要等待写入journal后返回
        boolean needSync = false;
        // 构造派发请求
        for (JournalLog journalLog : context.getLogs()) {
            journalLog.setStoreTime(storeTime);
            ByteBuf buffer = heapBufferPool.get();
            buffer.clear();
            buffers.add(Serializer.serialize(journalLog, buffer));
            logSize += journalLog.getSize();
//            if (journalLog.getType() == JournalLog.TYPE_TX_PRE_MESSAGE) {
//                needSync = true;
//            }
        }

        FlushRequest flushRequest = new FlushRequest();
        flushRequest.getWrite().begin();
        // 刷盘标示
        FlushPolicy flushPolicy = config.getJournalFlushPolicy();
        boolean flush = flushPolicy.isSync();

        // 判断是同步刷盘还是异步刷盘
        if (flush) {
            flushDiskTimeout = 0;
        } else {
            // 异步刷盘，通过大小和时间来判断是否要刷盘,size大小可能不精确，但是不影响逻辑
            appendFlush.addSize(logSize);
            flushDiskTimeout = flushPolicy.getFlushInterval();
            if (appendFlush.match(flushPolicy, SystemClock.now(), false)) {
                flush = true;
            }
        }
        if (needSync) {
            flushRequest.setLatch(new CountDownLatch(flush ? 2 : 1));
        }

        result.setCode(JMQCode.SE_PENDING);
        JournalEvent journalEvent = new JournalEvent(context, flushRequest, buffers, flush, null);
        boolean addFlag = flushingMessagesQueue.add(journalEvent, config.getWriteEnqueueTimeout());
        if (!addFlag) {
            // 入队失败，刷盘或复制过慢
            result.setCode(JMQCode.SE_ENQUEUE_SLOW);
            throw new JMQException(
                    String.format("it's timeout to add to journal queue,remain:%d, capacity:%d", flushingMessagesQueue.capacity(),
                            flushingMessagesQueue.capacity()), JMQCode.SE_APPEND_MESSAGE_SLOW.getCode());
        }
        if (needSync) {
            try {
                boolean flag = flushRequest.getLatch().await(config.getFlushTimeout(), TimeUnit.MILLISECONDS);
                if (!flag) {
                    // 刷盘复制过慢
                    throw new JMQException("it's timeout to wait for appending journal.", JMQCode.SE_FLUSH_TIMEOUT.getCode());
                }
            } catch (InterruptedException e) {
                throw new JMQException("the journal appending is interrupted.", JMQCode.SE_FLUSH_TIMEOUT.getCode());
            }
            result.setCode(JMQCode.SUCCESS);
        }
        // 增加计数器
        count.incrementAndGet();
    }

    protected PutResult appendJournalLog(final JournalLog journalLog) throws JMQException {
        final CountDownLatch waitLatch = new CountDownLatch(1);
        List<JournalLog> list = new ArrayList<JournalLog>(1);
        list.add(journalLog);
        StoreContext storeContext = new StoreContext(null, null, list, new Store.ProcessedCallback() {
            @Override
            public void onProcessed(StoreContext context) throws JMQException {
                waitLatch.countDown();
            }
        });
        try {
            appendJournalLog(storeContext);
        } catch (Exception e) {
            if (e instanceof JMQException) {
                throw (JMQException) e;
            }
            throw new JMQException(JMQCode.SE_IO_ERROR, e);
        }
        try {
            waitLatch.wait();
        } catch (InterruptedException e) {}
        return storeContext.getResult();
    }

    /**
     * 把消息写入磁盘并进行复制
     *
     * @param journalEvent 事件
     * @throws java.lang.Exception
     */
    protected void writeJournalLog(final JournalEvent journalEvent) throws Exception {
        if (isDiskFull()) {
            cleanupService.forceCleanup();
            if (isDiskFull()) {
                throw new JMQException(JMQCode.SE_DISK_FULL);
            }
        }

        final List<? extends JournalLog> journalLogs = journalEvent.context.getLogs();
        final List<ByteBuf> buffers = journalEvent.buffers;
        final boolean flush = journalEvent.flush;
        final FlushRequest flushRequest = journalEvent.flushRequest;
        int size = 0;
        long offset = 0;
        for (int i = 0; i < journalLogs.size(); i++) {
            JournalLog journalLog = journalLogs.get(i);
            ByteBuf buf = buffers.get(i);
            byte logType = journalLog.getType();
            if (logType == JournalLog.TYPE_REF_MESSAGE) {
                dealBrokerRefMessage((BrokerRefMessage) journalLog, buf);
            } else {
                // 获取最后的日志文件
                AppendFile file = skipBlankAndGetFile(buf.readableBytes());
                if (logType == JournalLog.TYPE_MESSAGE) {
                    dealBrokerMessage((BrokerMessage) journalLog, buf, file);
                } else if (logType == JournalLog.TYPE_TX_PREPARE) {
                    dealPrepare((BrokerPrepare) journalLog, buf, file);
                } else if (logType == JournalLog.TYPE_TX_PRE_MESSAGE) {
                    dealBrokerPreMessage((BrokerMessage) journalLog, buf, file);
                } else if (logType == JournalLog.TYPE_TX_COMMIT) {
                    dealCommit((BrokerCommit) journalLog, buf, file);
                } else if (logType == JournalLog.TYPE_TX_ROLLBACK) {
                    dealRollback((BrokerRollback) journalLog, buf, file);
                } else {
                    logger.error("err type:" + logType);
                }
            }
            size += journalLog.getSize();
            offset = journalLog.getJournalOffset();
        }

        // 回收缓冲区，如果出了异常或者超过512K，则由GC回收,复制中的BUFFER不能重用，可能还在进行网络处理
        for (int i = 0; i < journalLogs.size(); i++) {
            if (journalLogs.get(i).getSize() < 1024 * 512) {
                heapBufferPool.release(buffers.get(i));
            }
        }
        buffers.clear();

        journalEvent.context.getResult().setCode(JMQCode.SUCCESS);

        if (journalEvent.context.getCallback() != null) {
            journalEvent.context.getCallback().onProcessed(journalEvent.context);
        }

        // 设置日志偏移
        flushRequest.setOffset(offset);
        // 设置消息大小
        flushRequest.setSize(size);
        // 写入完成
        if (flushRequest.getLatch() != null)
            flushRequest.getLatch().countDown();
        if (flush) {
            // 同步刷盘
            commitManager.add(flushRequest);
        } else {
            flushRequest.getWrite().end();
        }

    }

    private void dealBrokerRefMessage(BrokerRefMessage refMessage, ByteBuf buf) throws Exception {
        //判断是否为同一个文件，不是同一个文件需要复制一份消息
        AppendFile file = journalDir.lastAndCreate();
        long refFileId = refMessage.getRefJournalOffset() - refMessage.getRefJournalOffset() % journalConfig.getDataSize();
        boolean needToTXMessage = false;
        if (refMessage.getRefJournalSize() < config.getTransactionNeedDispatchSize()) {
            needToTXMessage = true;
        }
        if (!needToTXMessage) {
            if (refFileId == file.id()) {
                //相同文件，空闲位置是否可以写下
                AppendFile tmpFile = skipBlankAndGetFile(buf.readableBytes());
                if (tmpFile.id() != refFileId) {
                    needToTXMessage = true;
                }
            } else {
                needToTXMessage = true;
            }
        }

        if (needToTXMessage) {
            RByteBuffer refBuf = null;
            try {
                refBuf = this.getJournalMessage(refMessage.getRefJournalOffset(), refMessage.getRefJournalSize());
                byte[] bytes = new byte[refBuf.remaining()];
                if (refBuf.hasArray()) {
                    System.arraycopy(refBuf.array(), refBuf.arrayOffset() + refBuf.position(), bytes, 0, bytes.length);
                } else {
                    refBuf.get(bytes);
                }
                buf = Unpooled.wrappedBuffer(bytes);
            } catch (Exception e) {
                throw new JMQException(JMQCode.SE_IO_ERROR, e);
            } finally {
                if (refBuf != null) {
                    refBuf.release();
                }
            }
            //改变了长度，需要重新判断是否可以写下
            file = skipBlankAndGetFile(buf.readableBytes());
            //
        }

        ConsumeQueue queue = queueManager.getConsumeQueue(refMessage.getTopic(), refMessage.getQueueId());
        // 设置日志位置
        refMessage.setJournalOffset(file.writePosition() + file.id());
        // 设置队列偏移量
        refMessage.setQueueOffset(queue.getOffset());
        if (needToTXMessage) {
            //修改类型
            buf.setByte(buf.readerIndex() + 4 + 2, JournalLog.TYPE_TX_MESSAGE);
            // 写日志位置和队列位置
            buf.setLong(buf.readerIndex() + 4 + 2 + 1 + 1 + 2 + 1, refMessage.getJournalOffset());
            buf.setLong(buf.readerIndex() + 4 + 2 + 1 + 1 + 2 + 1 + 8 + 1, refMessage.getQueueOffset());

        } else {
            // 写日志位置和队列位置
            buf.setLong(buf.readerIndex() + 4 + 2 + 1 + 2, refMessage.getQueueOffset());
            buf.setLong(buf.readerIndex() + 4 + 2 + 1 + 2 + 8, refMessage.getJournalOffset());
        }
        // 转换成ByteBuffer
        ByteBuffer buffer = buf.nioBuffer();
        int size = buffer.remaining();
        // 追加到文件
        file.append(buffer);

        if (logger.isTraceEnabled()) {
            logger.trace(String.format("prepare append msg topic:%s,queueId:%d,journalOffset:%d,queueOffset:%d", refMessage.getTopic(), refMessage.getQueueId(), refMessage.getJournalOffset(), refMessage.getQueueOffset()));
        }
        // 增加索引位置
        queue.updateOffset();
        DispatchRequest dispatchRequest = new DispatchRequest(refMessage, size, false, false);
        dispatchService.add(dispatchRequest, config.getDispatchEnqueueTimeout());
    }

    private void dealBrokerMessage(BrokerMessage message, ByteBuf buf, AppendFile file) throws Exception {
        ConsumeQueue queue ;
        queue = queueManager.getConsumeQueue(message.getTopic(), message.getQueueId());

        if (queue == null && message.getQueueId() == MessageQueue.HIGH_PRIORITY_QUEUE) {
            queue = queueManager.getAndCreateQueue(message.getTopic(), message.getQueueId());
        }
        // 设置日志位置
        message.setJournalOffset(file.writePosition() + file.id());
        if(message.getQueueOffset() ==0){
            // 设置队列偏移量
            message.setQueueOffset(queue.getOffset());
        }
        // 写日志位置和队列位置
        buf.setLong(buf.readerIndex() + 4 + 2 + 1 + 2 + 1, message.getJournalOffset());
        buf.setLong(buf.readerIndex() + 4 + 2 + 1 + 2 + 1 + 8 + 1, message.getQueueOffset());
        // 转换成ByteBuffer
        ByteBuffer buffer = buf.nioBuffer();
        // 追加到文件
        file.append(buffer);
        if (logger.isTraceEnabled()) {
            logger.trace(String.format("prepare append msg topic:%s,queueId:%d,journalOffset:%d,queueOffset:%d", message.getTopic(), message.getQueueId(), message.getJournalOffset(), message.getQueueOffset()));
        }
        // 增加索引位置
        queue.updateOffset();
        dispatchService.add(new DispatchRequest(message), config.getDispatchEnqueueTimeout());
    }

    private void dealBrokerPreMessage(BrokerMessage message, ByteBuf buf, AppendFile file) throws Exception {
        // 设置日志位置
        message.setJournalOffset(file.writePosition() + file.id());
        // 写日志位置
        buf.setLong(buf.readerIndex() + 4 + 2 + 1 + 1 + 2 + 1, message.getJournalOffset());
        // 转换成ByteBuffer
        ByteBuffer buffer = buf.nioBuffer();
        // 追加到文件
        file.append(buffer);
        if (logger.isTraceEnabled()) {
            logger.trace(String.format("prepare append msg topic:%s,queueId:%d,journalOffset:%d,queueOffset:%d", message.getTopic(), message.getQueueId(), message.getJournalOffset(), message.getQueueOffset()));
        }
    }

    private void dealPrepare(BrokerPrepare prepare, ByteBuf buf, AppendFile file)
            throws Exception {
        prepare.setJournalOffset(file.writePosition() + file.id());
        buf.setLong(buf.readerIndex() + 4 + 2 + 1, prepare.getJournalOffset());

        ByteBuffer buffer = buf.nioBuffer();
        file.append(buffer);
        if (logger.isTraceEnabled()) {
            logger.trace(String.format("deal prepare  topic:%s,txId:%s,journalOffset:%d", prepare.getTopic(), prepare
                    .getTxId(), prepare.getJournalOffset()));
        }
    }

    private void dealCommit(BrokerCommit commit, ByteBuf buf, AppendFile file)
            throws Exception {
        commit.setJournalOffset(file.writePosition() + file.id());
        buf.setLong(buf.readerIndex() + 4 + 2 + 1, commit.getJournalOffset());

        ByteBuffer buffer = buf.nioBuffer();
        file.append(buffer);
        if (logger.isTraceEnabled()) {
            logger.trace(String.format("deal commit  topic:%s,txId:%s,journalOffset:%d", commit.getTopic(), commit
                    .getTxId(), commit.getJournalOffset()));
        }
    }

    private void dealRollback(BrokerRollback rollback, ByteBuf buf, AppendFile file)
            throws Exception {
        rollback.setJournalOffset(file.writePosition() + file.id());
        buf.setLong(buf.readerIndex() + 4 + 2 + 1, rollback.getJournalOffset());

        ByteBuffer buffer = buf.nioBuffer();
        file.append(buffer);

        if (logger.isTraceEnabled()) {
            logger.trace(String.format("deal rollback  topic:%s,txId:%s,journalOffset:%d", rollback.getTopic(), rollback
                    .getTxId(), rollback.getJournalOffset()));
        }
    }

    private AppendFile skipBlankAndGetFile(int size) throws IOException, JMQException {
        AppendFile file = journalDir.lastAndCreate();

        // 计算剩余空间
        int remain = (int) (file.length() - file.writePosition()-file.headSize());
        // 判断剩余空间是否足够
        if (remain < size) {
            // 不够，大于等于8字节，则可以写入一个空白标示
            long offset = file.writePosition() + file.id();
            ByteBuffer blankBuffer = ByteBuffer.allocate(remain);
            if (remain >= MESSAGE_HEAD_LEN) {
                blankBuffer.putInt(remain);
                blankBuffer.putShort(MAGIC_BLANK_CODE);
            }
            // 准备好数据
            blankBuffer.position(remain);
            blankBuffer.flip();
            file.append(blankBuffer);
            // 刷盘
            file.flush();
            appendFlush.reset(SystemClock.getInstance().now());
            // 创建新日志文件
            file = journalDir.create();
        }
        return file;
    }

    /**
     * 获取消息
     *
     * @param offset 日志全局偏移量
     * @param size   数据大小
     * @return 消息缓冲区
     * @throws Exception
     */
    public RByteBuffer getJournalMessage(final long offset, final int size) throws Exception {
        if (size > 0) {
            AppendFile file = journalDir.get(offset);
            if (file != null) {
                return new RByteBuffer(file.read((int) (offset - file.id()), size), file);
            }
        }
        return null;
    }

    /**
     * 获取日志文件最大偏移量
     *
     * @return 最大偏移量
     */
    public long getMaxOffset() {
        AppendFile file = journalDir.last();
        if (file == null) {
            return 0;
        }
        return file.id() + file.writePosition();
    }

    /**
     * 获取最小偏移量
     *
     * @return 最小偏移量
     */
    public long getMinOffset() {
        AppendFile file = journalDir.first();
        if (file == null) {
            return 0;
        }
        return file.id();
    }

    /**
     * 删除老数据
     *
     * @param journalOffset       截止的偏移量
     * @param retentionTime       文件保留时间(毫秒)
     * @param deleteFilesInterval 删除多个文件时间间隔(毫秒)
     * @param maxDelNum           最大删除文件数
     * @throws IOException
     */
    public void truncateBefore(final long journalOffset, final long retentionTime, final int deleteFilesInterval,
                               final int maxDelNum) throws IOException {
        boolean flag;
        int i = 0;
        while (isStarted()) {
            flag = journalDir.deleteOldest(journalOffset, retentionTime);
            if (!flag) {
                if (i == 0 && maxDelNum > 0) {
                    if (logger.isInfoEnabled()) {
                        logger.info("truncate before journal offset:" + journalOffset + ",maxDelNum:" + maxDelNum);
                    }
                }
                break;
            }
            if (deleteFilesInterval > 0) {
                await(deleteFilesInterval);
            }
            i++;
            if (maxDelNum > 0 && maxDelNum <= i) {
                break;
            }
        }
    }

    /**
     * 删除所有文件
     *
     * @param deleteFilesInterval 删除文件间隔
     * @throws IOException
     */
    public void truncateAll(final int deleteFilesInterval) throws IOException {
        boolean flag;
        while (isStarted()) {
            flag = journalDir.deleteOldest(this.getMaxOffset() + journalConfig.getDataSize(), 0);
            if (!flag) {
                break;
            }

            if (deleteFilesInterval > 0) {
                await(deleteFilesInterval);
            }
        }
    }

    /**
     * 判断磁盘是否满
     *
     * @return 磁盘满标示
     */
    public boolean isDiskFull() {
        long used = appendFileManager.getFileStat().getAppendFileLength() + queueManager.getFileStat()
                .getAppendFileLength();
        return used >= config.getMaxDiskSpace();
    }

    /**
     * 获取文件统计信息
     *
     * @return 文件统计信息
     */
    public FileStat getDiskFileStat() {
        JournalFileStat fileStat = new JournalFileStat();
        FileStat journalFileStat = appendFileManager.getFileStat();
        FileStat queueFileStat = queueManager.getFileStat();
        fileStat.setPerformanceStat(journalFileStat.slice());

        int journalFiles = journalFileStat.getAppendFiles();
        long journalFileSize = journalFileStat.getAppendFileLength();
        int journalMappedFiles = journalFileStat.getMappedFiles();
        long journalMappedFileSize = journalFileStat.getMappedMemory();

        int queueFiles = queueFileStat.getAppendFiles();
        long queueFileSize = queueFileStat.getAppendFileLength();
        int queueMappedFiles = queueFileStat.getMappedFiles();
        long queueMappedFileSize = queueFileStat.getMappedMemory();


        fileStat.addAppendFile(journalFileSize + queueFileSize, journalFiles + queueFiles);
        fileStat.addMappedFile(journalMappedFileSize + queueMappedFileSize, journalMappedFiles + queueMappedFiles);

        return fileStat;
    }

    public class JournalFileStat extends FileStat {
        protected void addAppendFile(long size, int count) {
            if (size > 0) {
                appendFileLength.addAndGet(size);
            }
            if (count > 0) {
                appendFiles.addAndGet(count);
            }
        }

        protected void addMappedFile(long size, int count) {
            if (size > 0) {
                mappedMemory.addAndGet(size);
            }
            if (count > 0) {
                mappedFiles.addAndGet(count);
            }
        }

        protected void setPerformanceStat(FileStat.FileStatSlice stat) {
            readStat = stat;
        }
    }

    public void setDispatchService(DispatchService dispatchService) {
        this.dispatchService = dispatchService;
    }

    public void setQueueManager(QueueManager queueManager) {
        this.queueManager = queueManager;
    }

    public AppendDirectory getJournalDir() {
        return journalDir;
    }

    public AppendFileManager getAppendFileManager() {
        return appendFileManager;
    }

    public CommitManager getCommitManager() {
        return commitManager;
    }

    /**
     * 消息时间
     */
    protected class JournalEvent {
        // 门闩
        protected final CountDownLatch writeLatch;
        // 消息
        protected final StoreContext context;
        // 缓冲区
        protected final List<ByteBuf> buffers;
        protected final FlushRequest flushRequest;
        // 刷盘标示
        protected final boolean flush;

        public JournalEvent(final StoreContext context, final FlushRequest flushRequest,
                            final List<ByteBuf> buffers, final boolean flush, final CountDownLatch writeLatch) {
            this.writeLatch = writeLatch;
            this.context = context;
            this.buffers = buffers;
            this.flush = flush;
            this.flushRequest = flushRequest;
        }
    }

    public void addTransaction(BrokerPrepare prepare) throws JMQException {
        if (prepare == null) {
            return;
        }

        InflightTx old = inflights.putIfAbsent(prepare.getTxId(), new InflightTx(prepare));
        if (old != null) {
            throw new JMQException("txId is duplicated. txId:" + prepare.getTxId(), JMQCode.CN_UNKNOWN_ERROR.getCode());
        }
        changeInflightCount(prepare.getTopic(), 1);
    }

    public boolean isLimitedAddTransaction(String topic) {
        boolean bRet = false;
        AtomicInteger counter = inflightCounter.get(topic);

        if (counter != null && counter.get() >= config.getTransactionLimit()) {
            bRet = true;
        }
        return bRet;
    }

    public InflightTx removeTransaction(String txId) {
        InflightTx inflightTx = inflights.remove(txId);
        if (inflightTx != null) {
            changeInflightCount(inflightTx.getTopic(), -1);
        }
        return inflightTx;
    }

    public void addInflitTx(InflightTx tx) {
        inflights.put(tx.getTxId(), tx);
    }

    public void addBrokerRefMessage(String txId, BrokerRefMessage refMessage) throws JMQException {
        if (txId == null || refMessage == null) {
            logger.warn(String.format("txId or messages is empty! txId=%s, refMessage=%s",
                    txId, refMessage));
            return;
        }
        InflightTx inflightTx = inflights.get(txId);
        if (inflightTx == null) {
            throw new JMQException("txid is not exsits:" + txId, JMQCode.CN_UNKNOWN_ERROR.getCode());
        }

        List<BrokerRefMessage> messages = new ArrayList<BrokerRefMessage>(1);
        messages.add(refMessage);
        inflightTx.addMessages(messages);
    }

    private void changeInflightCount(String topic, int count) {
        if (topic == null) {
            return;
        }
        AtomicInteger counter = inflightCounter.get(topic);

        if ((counter == null && count > 0) || (counter != null && counter.get() < 0)) {
            counter = new AtomicInteger(0);
            AtomicInteger old = inflightCounter.putIfAbsent(topic, new AtomicInteger(count));
            if (old != null) {
                counter = old;
            }
        }

        if (counter != null) {
            counter.addAndGet(count);
        }
    }

    public long getTxMinJournalOffset() {
        long minJournalOffset = -1;
        if (inflights != null) {
            for (InflightTx inflightTx : inflights.values()) {
                BrokerPrepare prepare = inflightTx.getPrepare();
                if (prepare != null) {
                    if (minJournalOffset < 0 || prepare.getJournalOffset() < minJournalOffset) {
                        minJournalOffset = prepare.getJournalOffset();
                    }
                }
            }
        }

        return minJournalOffset;
    }

    public BrokerPrepare lockAndGetTimeoutTransaction(String topic, String owner) {
        if (topic != null && !inflights.isEmpty()) {
            AtomicInteger count = inflightCounter.get(topic);
            if (count != null && count.get() > 0) {
                List<InflightTx> txes = new ArrayList<InflightTx>();
                //获取到所有超时的事务
                for (InflightTx tx : inflights.values()) {
                    if (topic.equals(tx.getTopic()) && tx.isTimeout() && tx.needFeedback()) {
                        txes.add(tx);
                    }
                }

                if (txes.size() > 0) {
                    //以查询次数作为排序，避免重复拿到同一个事务
                    Collections.sort(txes);
                    for (InflightTx tx : txes) {
                        if (tx.tryLock(owner)) {
                            //返回
                            return tx.getPrepare();
                        }
                    }
                }
            }
        }

        return null;
    }

    public void unLockInflightTransaction(String txId) {
        if (txId != null && !inflights.isEmpty()) {
            InflightTx tx = inflights.get(txId);
            if (tx != null) {
                tx.unLock();
            }
        }
    }

    public BrokerPrepare getBrokerPrepare(String txId) {
        InflightTx inflightTx = inflights.get(txId);
        if (inflightTx != null) {
            return inflightTx.getPrepare();
        }
        return null;
    }

    public List<BrokerPrepare> getInflightTransactions(String topic, int count) {
        if (topic != null && !inflights.isEmpty()) {
            AtomicInteger inflightCount = inflightCounter.get(topic);
            if (inflightCount != null && inflightCount.get() > 0) {
                List<BrokerPrepare> prepares = new ArrayList<BrokerPrepare>();
                int counter = 0;
                for (InflightTx inflightTx : inflights.values()) {
                    BrokerPrepare prepare = inflightTx.getPrepare();
                    if (prepare != null && topic.equals(prepare.getTopic())) {
                        prepares.add(prepare);
                        counter++;
                    }

                    if (count > 0 && (counter >= count || counter >= inflightCount.get())) {
                        break;
                    }
                }
                return prepares;
            }

        }

        return null;
    }

    public List<BrokerPrepare> cleanExpiredTransaction() {
        if (inflights.isEmpty()) {
            return Collections.emptyList();
        }

        List<BrokerPrepare> expired = new ArrayList<BrokerPrepare>();
        for (InflightTx inflightTx : inflights.values()) {
            if (canClean(inflightTx)) {
                BrokerPrepare prepare = inflightTx.getPrepare();
                try {
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("Transaction[%s] expired will rollback it.", prepare.getTxId()));
                    }
                    BrokerRollback rollback = new BrokerRollback();
                    rollback.setTopic(prepare.getTopic());
                    rollback.setTxId(prepare.getTxId());
                    //回滚事务
                    appendJournalLog(rollback);
                    //删除事务
                    inflights.remove(prepare.getTxId());
                    changeInflightCount(prepare.getTopic(), -1);
                    expired.add(prepare);
                } catch (Throwable e) {
                    logger.error(String.format("Clean expired transaction error. BrokerPrepare=%s", prepare), e);
                }
            }
        }

        cleanExpiredLockedTransaction();

        return expired;
    }

    private void cleanExpiredLockedTransaction() {
        for (InflightTx inflightTx : inflights.values()) {
            // 加锁超时的释放掉
            if (!inflightTx.isFree()) {
                inflightTx.expireUnLock(SystemClock.now());
            }
        }
    }

    private boolean canClean(final InflightTx inflightTx) {
        long current = SystemClock.now();
        //最大查询次数
        int queryLimit = config.getTransactionQueryLimit();
        // 保存截止时间
        long reserveDeadline = inflightTx.getStartTime() + config.getTransactionReserveTime();
        // 是否超过最大查询次数
        boolean exceedQueryLimit = queryLimit != StoreConfig.TX_QUERY_LIMIT_INFINITE && inflightTx.getSuccessCount().get() > queryLimit;

        return inflightTx.isTimeout() && (reserveDeadline < current || exceedQueryLimit || !inflightTx.needFeedback());
    }

    public void onTransactionFeedback(String txId) {
        InflightTx tx = inflights.get(txId);
        if (tx != null) {
            tx.onFeedback();
        }
    }

    protected class InflightTx implements Comparable {
        private BrokerPrepare prepare;
        private AtomicReference<OwnerShip> ownerShip = new AtomicReference<OwnerShip>();
        private AtomicInteger totalQueryCount = new AtomicInteger(0);
        private AtomicInteger successCount = new AtomicInteger(0);

        public InflightTx(BrokerPrepare prepare) {
            this.prepare = prepare;
        }

        public BrokerPrepare getPrepare() {
            return prepare;
        }

        public void setPrepare(BrokerPrepare prepare) {
            this.prepare = prepare;
        }

        public boolean tryLock(String owner) {
            if (isFree()) {
                OwnerShip ownerShipInstance = new OwnerShip(owner, 0);
                long now = SystemClock.now();
                ownerShipInstance.setExpireTime(now + config.getTransactionFeadbackLockedTimeout());
                ownerShipInstance.setCreateTime(now);
                if (ownerShip.compareAndSet(null, ownerShipInstance)) {
                    totalQueryCount.incrementAndGet();
                    return true;
                }
            }
            return false;
        }

        public void onFeedback() {
            successCount.incrementAndGet();
        }

        public void unLock() {
            ownerShip.set(null);
        }

        public boolean isFree() {
            return ownerShip.get() == null;
        }


        public String getTopic() {
            return prepare != null ? prepare.getTopic() : null;
        }

        public String getTxId() {
            return prepare != null ? prepare.getTxId() : null;
        }

        public void addMessages(List<BrokerRefMessage> messages) {
            if (prepare != null) {
                prepare.addMessages(messages);
            }
        }

        public void addMessage(BrokerRefMessage message) {
            if (prepare != null) {
                prepare.addMessage(message);
            }
        }

        public AtomicInteger getTotalQueryCount() {
            return totalQueryCount;
        }

        public void setTotalQueryCount(AtomicInteger totalQueryCount) {
            this.totalQueryCount = totalQueryCount;
        }

        public AtomicInteger getSuccessCount() {
            return successCount;
        }

        public void setSuccessCount(AtomicInteger successCount) {
            this.successCount = successCount;
        }

        public boolean isTimeout() {
            if (prepare != null) {
                return SystemClock.now() > (prepare.getTimeout() + prepare.getStartTime());
            }
            return false;
        }

        public boolean needFeedback() {
            if (prepare != null) {
                return prepare.getTimeoutAction() == TxPrepare.TIME_OUT_ACTION_FEEDBACK;
            }

            return false;
        }

        public long getStartTime() {
            if (prepare != null) {
                return prepare.getStartTime();
            }
            return 0;
        }

        public void expireUnLock(long now) {

            OwnerShip ownerShipInstance = ownerShip.get();
            if (ownerShipInstance != null) {
                // 判断是否过期
                if (ownerShipInstance.isExpire(now)) {
                    logger.warn("--now:" + now + " ,clean locked inflightTx, topic is " + prepare.getTopic() + " and transaction id is " + prepare.getTxId());
                    unLock();
                }
            }

        }

        @Override
        public int compareTo(Object o) {
            if (o instanceof InflightTx) {
                return this.getTotalQueryCount().get() - ((InflightTx) o).getTotalQueryCount().get();
            }
            return 0;
        }
    }

    public Lock getLock() {
        return super.getWriteLock();
    }

    /**
     * 写盘，顺序追加，单线程控制
     */
    protected class FlushingMessagesQueueHandler implements RingBuffer.EventHandler {
        @Override
        public void onEvent(final Object[] elements) throws Exception {
            // 遍历提交的数据
            for (Object element : elements) {
                writeJournalLog((JournalEvent) element);
            }
        }

        @Override
        public void onException(Throwable e) {
            logger.error("flush message error", e);
            if (isStarted()) {
                if (eventManager != null) {
                    eventManager.add(new StoreEvent(StoreEvent.EventType.EXCEPTION, e));
                }
            }
        }

        @Override
        public int getBatchSize() {
            return 0;
        }

        @Override
        public long getInterval() {
            return 0;
        }
    }
}