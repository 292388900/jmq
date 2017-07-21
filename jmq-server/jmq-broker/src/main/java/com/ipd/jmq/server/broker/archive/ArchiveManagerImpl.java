package com.ipd.jmq.server.broker.archive;

/**
 * @author songzhimao
 * @date 2017/7/10
 */

import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.message.MessageId;
import com.ipd.jmq.common.message.MessageLocation;
import com.ipd.jmq.common.model.ConsumeHistory;
import com.ipd.jmq.common.model.MessageInfo;
import com.ipd.jmq.common.network.v3.session.Connection;
import com.ipd.jmq.server.broker.log.serializer.ConsumeHistorySerializer;
import com.ipd.jmq.server.broker.log.serializer.MessageInfoSerializer;
import com.ipd.jmq.server.store.StoreConfig;
import com.ipd.jmq.toolkit.concurrent.RingBuffer;
import com.ipd.jmq.toolkit.io.Files;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.network.Ipv4;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.slf4j.Logger;

public class ArchiveManagerImpl extends Service implements ArchiveManager {
    private static final Logger logger = LoggerFactory.getLogger(ArchiveManager.class);
    // 临时文件后缀，防止读写归档文件冲突
    private static final String TMP_FILE_SUFFIX = ".tmp";
    // 正式文件后缀
    private static final String LOG_FILE_SUFFIX = ".log";
    // 存储的配置文件
    private StoreConfig storeConfig;
    // 生产的归档日志路径
    private File producePath;
    // 消费的归档日志路径
    private File consumePath;
    // 归档根路径
    private File statFile;
    // 全局索引
    private ArchiveStat archiveStat;
    // 生产文件
    private ArchiveFileAppender produceArchiveAppender;
    // 消费文件
    private ArchiveFileAppender consumeArchiveAppender;
    // 生产消息归档序列化
    private MessageInfoSerializer messageInfoSerializer = new MessageInfoSerializer();
    // 消费消息归档序列化
    private ConsumeHistorySerializer consumeHistorySerializer = new ConsumeHistorySerializer();
    // 生产缓存区
    private RingBuffer produceRingBuffer;
    // 消费缓存区
    private RingBuffer consumeRingBuffer;

    public ArchiveManagerImpl() {
    }

    public ArchiveManagerImpl(StoreConfig storeConfig) {
        if (storeConfig == null) {
            throw new IllegalArgumentException("storeConfig can not be null");
        }
        this.storeConfig = storeConfig;
        this.producePath = new File(storeConfig.getArchiveDirectory(), "produce");
        this.consumePath = new File(storeConfig.getArchiveDirectory(), "consume");
        this.statFile = new File(storeConfig.getArchiveDirectory(), "/archivestat");
    }

    @Override
    protected void validate() throws Exception {
        super.validate();
        if (produceRingBuffer == null) {
            produceRingBuffer =
                    new RingBuffer(storeConfig.getArchiveCapacity(), new produceHandler(), "archProduceBuffer");
        }
        if (consumeRingBuffer == null) {
            consumeRingBuffer =
                    new RingBuffer(storeConfig.getArchiveCapacity(), new consumeHandler(), "archConsumeBuffer");
        }

        Preconditions.checkState(Files.createDirectory(producePath), "create directory error. " + producePath.getPath());
        Preconditions.checkState(Files.createDirectory(consumePath), "create directory error. " + consumePath.getPath());
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        // 索引文件
        archiveStat = new ArchiveStat(statFile);
        archiveStat.start();

        // 获取最大ID的生产归档文件
        ArchiveFile maxProduceFile = getMaxIdFile(producePath.listFiles());
        // 获取最大ID的消费归档文件
        ArchiveFile maxConsumeFile = getMaxIdFile(consumePath.listFiles());

        // 初始化索引数据
        boolean success = false;
        try {
            if (statFile.length() > 0) {
                archiveStat.read();
                success = true;
                logger.info("archiveStat is read from file");
            }
        } catch (IOException e) {
            // 失败,则尝试恢复数据
            logger.warn("read archive stat file error,try to recover.", e);
        }
        if (!success) {
            archiveStat.setProduceFileId(maxProduceFile.getFileId());
            archiveStat.setProduceOffset(maxProduceFile.getLength());
            archiveStat.setConsumeFileId(maxConsumeFile.getFileId());
            archiveStat.setConsumeOffset(maxConsumeFile.getLength());
        }

        // 初始化ringbuffer
        produceRingBuffer.start();
        consumeRingBuffer.start();

        // 如果最后一个文件是临时文件,继续读写;如果不是临时文件，则从下一个编号开始读写
        if (maxProduceFile.getExtension() == null) {
            archiveStat.setProduceFileId(archiveStat.getProduceFileId() + 1);
            archiveStat.setProduceOffset(0);
        }
        if (maxConsumeFile.getExtension() == null) {
            archiveStat.setConsumeFileId(archiveStat.getConsumeFileId() + 1);
            archiveStat.setConsumeOffset(0);
        }

        produceArchiveAppender =
                new ArchiveFileAppender(producePath, archiveStat.getProduceFileId(), archiveStat.getProduceOffset());
        produceArchiveAppender.start();
        consumeArchiveAppender =
                new ArchiveFileAppender(consumePath, archiveStat.getConsumeFileId(), archiveStat.getConsumeOffset());
        consumeArchiveAppender.start();
        logger.info("ArchiveManager is started.");
    }

    @Override
    protected void doStop() {
        super.doStop();

        // 先把数据写入硬盘
        Close.close(produceRingBuffer).close(consumeRingBuffer).close(produceArchiveAppender)
                .close(consumeArchiveAppender).close(archiveStat);

        logger.info("ArchiveManager is stopped.");
    }

    /**
     * 写生产者归档文件
     *
     * @param message 消息
     */
    @Override
    public void writeProduce(final BrokerMessage message) {
        if (message == null || !isStarted()) {
            return;
        }
        MessageInfo messageInfo = new MessageInfo();
        messageInfo.setMessageId(message.getMessageId().getMessageId());
        messageInfo.setApp(message.getApp());
        messageInfo.setBusinessId(message.getBusinessId());
        messageInfo.setTopic(message.getTopic());
        messageInfo.setJournalOffset(message.getJournalOffset());
        messageInfo.setJournalSize(message.getSize());
        messageInfo.setClientIp(Ipv4.toIp(message.getClientAddress()));
        messageInfo.setSendTime(message.getSendTime());
        messageInfo.setReceiveTime(message.getReceiveTime());

        if (!produceRingBuffer.add(messageInfo, storeConfig.getArchiveProduceEnqueueTimeout())) {
            logger.error("messageInfo add to buffer timeout");
        }
    }

    /**
     * 写消费者归档文件
     *
     * @param connection 连接
     * @param location   消费位置
     */
    @Override
    public void writeConsume(final Connection connection, final MessageLocation location) {
        if (!isStarted() || location == null || connection == null) {
            return;
        }
        MessageId messageId = new MessageId(location.getAddress(), location.getJournalOffset());
        ConsumeHistory consumeHistory = new ConsumeHistory();
        consumeHistory.setApp(connection.getApp());
        consumeHistory.setConsumeTime(SystemClock.getInstance().now());
        consumeHistory.setJournalOffset(location.getJournalOffset() < 0 ? 0 : location.getJournalOffset());
        consumeHistory.setTopic(location.getTopic());
        consumeHistory.setMessageId(messageId.getMessageId());
        consumeHistory.setClientIp(Ipv4.toIp(connection.getAddress()));
        if (!consumeRingBuffer.add(consumeHistory, storeConfig.getArchiveConsumeEnqueueTimeout())) {
            logger.error("consumeHistory add to buffer timeout");
        }
    }

    /**
     * 写消费者归档文件
     *
     * @param location 消费位置
     */
    @Override
    public void writeConsume(final String app, final String clientIp, final MessageLocation location) {
        if (!isStarted() || location == null) {
            return;
        }
        MessageId messageId = new MessageId(location.getAddress(), location.getJournalOffset());
        ConsumeHistory consumeHistory = new ConsumeHistory();
        consumeHistory.setApp(app);
        consumeHistory.setConsumeTime(SystemClock.getInstance().now());
        consumeHistory.setJournalOffset(location.getJournalOffset() < 0 ? 0 : location.getJournalOffset());
        consumeHistory.setTopic(location.getTopic());
        consumeHistory.setMessageId(messageId.getMessageId());
        consumeHistory.setClientIp(clientIp);
        if (!consumeRingBuffer.add(consumeHistory, storeConfig.getArchiveConsumeEnqueueTimeout())) {
            logger.error("consumeHistory add to buffer timeout");
        }
    }

    /**
     * 生产归档记录刷盘到文件，满足数据条数或到达时间点都会触发
     *
     * @param targets 生产归档记录
     * @param buffer  缓冲区
     * @throws Exception
     */
    @Override
    public void flushProduce(Object[] targets, ByteBuffer buffer) throws Exception {
        MessageInfo messageInfo;
        long now;
        boolean sync = false;
        // 缓冲区复位
        buffer.clear();

        // 判断文件刷盘时间是否初始化
        if (produceArchiveAppender.getFlushTime() <= 0) {
            produceArchiveAppender.setFlushTime(SystemClock.getInstance().now());
        }
        long lastFlushTime = produceArchiveAppender.getFlushTime();
        // 判断是否有数据
        if (targets != null && targets.length > 0) {
            // 循环写入数据
            for (Object target : targets) {
                messageInfo = (MessageInfo) target;
                messageInfoSerializer.encode(messageInfo, buffer);

                // 判断文件大小是否满足分割大小
                if ((produceArchiveAppender.getOffset() + buffer.remaining() >= storeConfig
                        .getArchiveProduceFileSize())) {
                    // 当前文件满足Rolling的大小或时间间隔
                    sync = true;
                    produceArchiveAppender.append(buffer);
                    produceArchiveAppender.flushAndRolling();
                } else if (buffer.capacity() - buffer.position() < 1024) {
                    // 缓冲区不能再追加写入了，把数据交换到文件
                    produceArchiveAppender.append(buffer);
                }
            }
            // 有数据没有写完
            if (buffer.position() > 0) {
                produceArchiveAppender.append(buffer);
            }

            // 判断是否满足分割文件的时间
            now = SystemClock.now();
            if (produceArchiveAppender.offset > 0 && now - lastFlushTime > storeConfig.getArchiveRollingInterval()) {
                sync = true;
                produceArchiveAppender.flushAndRolling();
            }

            archiveStat.write(produceArchiveAppender.getFileId(), produceArchiveAppender.getOffset(),
                    consumeArchiveAppender.getFileId(), consumeArchiveAppender.getOffset(), sync);
        } else if (produceArchiveAppender.getOffset() > 0) {
            // 达到时间间隔触发（默认5分钟）
            sync = true;
            produceArchiveAppender.flushAndRolling();
            archiveStat.write(produceArchiveAppender.getFileId(), produceArchiveAppender.getOffset(),
                    consumeArchiveAppender.getFileId(), consumeArchiveAppender.getOffset(), sync);
        }

    }

    /**
     * 消费者归档文件刷盘
     *
     * @param targets 消费归档记录
     * @param buffer  缓冲区
     * @throws Exception
     */
    @Override
    public void flushConsume(Object[] targets, ByteBuffer buffer) throws Exception {

        ConsumeHistory consumeHistory;
        long now;
        boolean sync = false;
        // 缓冲区复位
        buffer.clear();

        // 判断文件刷盘时间是否初始化
        if (consumeArchiveAppender.getFlushTime() <= 0) {
            consumeArchiveAppender.setFlushTime(SystemClock.getInstance().now());
        }
        long lastFlushTime = consumeArchiveAppender.getFlushTime();
        // 判断是否有数据
        if (targets != null && targets.length > 0) {
            // 循环写入数据
            for (Object target : targets) {
                consumeHistory = (ConsumeHistory) target;
                consumeHistorySerializer.encode(consumeHistory, buffer);

                // 判断文件大小是否满足分割大小
                if ((consumeArchiveAppender.getOffset() + buffer.remaining() >= storeConfig
                        .getArchiveConsumeFileSize())) {
                    sync = true;
                    consumeArchiveAppender.append(buffer);
                    consumeArchiveAppender.flushAndRolling();
                } else if (buffer.capacity() - buffer.position() < 1024) {
                    // 缓冲区不能再追加写入了，把数据交换到文件
                    consumeArchiveAppender.append(buffer);
                }
            }
            // 有数据没有写完
            if (buffer.position() > 0) {
                consumeArchiveAppender.append(buffer);
            }

            now = SystemClock.now();
            if (consumeArchiveAppender.offset > 0 && now - lastFlushTime > storeConfig.getArchiveRollingInterval()) {
                sync = true;
                consumeArchiveAppender.flushAndRolling();
            }
            archiveStat.write(produceArchiveAppender.getFileId(), produceArchiveAppender.getOffset(),
                    consumeArchiveAppender.getFileId(), consumeArchiveAppender.getOffset(), sync);
        } else if (consumeArchiveAppender.getOffset() > 0) {
            sync = true;
            // 达到时间间隔触发（默认5分钟）
            consumeArchiveAppender.flushAndRolling();
            archiveStat.write(produceArchiveAppender.getFileId(), produceArchiveAppender.getOffset(),
                    consumeArchiveAppender.getFileId(), consumeArchiveAppender.getOffset(), sync);
        }
    }

    /**
     * 获取最大ID文件
     *
     * @param files 文件列表
     * @return 最大ID文件
     */

    protected ArchiveFile getMaxIdFile(File[] files) {
        ArchiveFile maxIdFile = new ArchiveFile(null, 1, null, 0);
        if (files == null || files.length == 0) {
            return maxIdFile;
        }
        long max = 0;
        int pos;
        long fileId;
        String extension;
        String name;
        for (File file : files) {
            name = file.getName();
            pos = name.lastIndexOf('.');
            try {
                if (pos > 0) {
                    fileId = Long.parseLong(name.substring(0, pos));
                    extension = name.substring(pos);
                } else {
                    continue;
                }
            } catch (NumberFormatException ignored) {
                continue;
            }
            if (fileId > max) {
                max = fileId;
                maxIdFile.setFile(file);
                maxIdFile.setFileId(max);
                maxIdFile.setExtension(extension);
                maxIdFile.setLength(file.length());
            }
        }
        return maxIdFile;

    }

    /**
     * 生产处理器
     */
    protected class produceHandler implements RingBuffer.EventHandler {

        private ByteBuffer buffer = ByteBuffer.allocate(storeConfig.getArchiveBufferSize());

        @Override
        public void onEvent(Object[] elements) throws Exception {
            flushProduce(elements, buffer);
        }

        @Override
        public void onException(Throwable exception) {
            logger.error(exception.getMessage(), exception);
        }

        @Override
        public int getBatchSize() {
            return storeConfig.getArchiveProduceThreshold();
        }

        @Override
        public long getInterval() {
            return storeConfig.getArchiveRollingInterval();
        }

    }

    /**
     * 消费处理器
     */
    protected class consumeHandler implements RingBuffer.EventHandler {
        private ByteBuffer buffer = ByteBuffer.allocate(storeConfig.getArchiveBufferSize());

        @Override
        public void onEvent(Object[] elements) throws Exception {
            flushConsume(elements, buffer);
        }

        @Override
        public void onException(Throwable exception) {
            logger.error(exception.getMessage(), exception);
        }

        @Override
        public int getBatchSize() {
            return storeConfig.getArchiveConsumeThreshold();
        }

        @Override
        public long getInterval() {
            return storeConfig.getArchiveRollingInterval();
        }

    }

    /**
     * 存储位置信息，双写
     */
    protected class ArchiveStat extends Service {
        // 记录当前生产者的文件名
        private long produceFileId = 1;
        // 记录当前生成者日志的位置
        private long produceOffset = 0;
        // 记录当前消费者的文件名
        private long consumeFileId = 1;
        // 记录当前消费者日志的位置
        private long consumeOffset = 0;
        // 校验和
        private long checksum;
        // 归档根路径
        private File statFile;
        // 索引文件读写
        private RandomAccessFile statRaf;

        public ArchiveStat(File statFile) {
            if (statFile == null) {
                throw new IllegalArgumentException("statFile can not be null");
            }
            this.statFile = statFile;
        }

        public long getProduceFileId() {
            return produceFileId;
        }

        public void setProduceFileId(long produceFileId) {
            this.produceFileId = produceFileId;
        }

        public long getConsumeFileId() {
            return consumeFileId;
        }

        public void setConsumeFileId(long consumeFileId) {
            this.consumeFileId = consumeFileId;
        }

        public long getProduceOffset() {
            return produceOffset;
        }

        public void setProduceOffset(long produceOffset) {
            this.produceOffset = produceOffset;
        }

        public long getConsumeOffset() {
            return consumeOffset;
        }

        public void setConsumeOffset(long consumeOffset) {
            this.consumeOffset = consumeOffset;
        }

        @Override
        public int hashCode() {
            int result = (int) (produceFileId ^ (produceFileId >>> 32));
            result = 31 * result + (int) (produceOffset ^ (produceOffset >>> 32));
            result = 31 * result + (int) (consumeFileId ^ (consumeFileId >>> 32));
            result = 31 * result + (int) (consumeOffset ^ (consumeOffset >>> 32));
            return result;
        }

        @Override
        protected void doStart() throws Exception {
            super.doStart();
            if (!Files.createFile(statFile)) {
                throw new IOException("create file error." + statFile.getPath());
            }
            this.statRaf = new RandomAccessFile(statFile, "rw");
        }

        @Override
        protected void doStop() {
            try {
                flush(true);
            } catch (IOException e) {
                logger.error("write archive stat error.", e);
            } finally {
                Close.close(statRaf);
                statRaf = null;
            }
            super.doStop();
        }

        /**
         * 读取数据
         *
         * @throws IOException
         */
        public void read() throws IOException {
            getWriteLock().lock();
            try {
                if (!isStarted()) {
                    throw new IOException("archive stat file is closed.");
                }
                if (statRaf.length() == 0) {
                    throw new IOException("archive stat file is empty." + statFile.getPath());
                }
                statRaf.seek(0);
                for (int i = 0; i < 2; i++) {
                    produceFileId = statRaf.readLong();
                    produceOffset = statRaf.readLong();
                    consumeFileId = statRaf.readLong();
                    consumeOffset = statRaf.readLong();
                    checksum = statRaf.readLong();
                    if (hashCode() == checksum) {
                        return;
                    }
                }
            } finally {
                getWriteLock().unlock();
            }
            throw new IOException("archive stat file is damaged." + statFile.getPath());
        }

        /**
         * 双写数据
         *
         * @param produceFileId 归档生产文件ID
         * @param produceOffset 归档生产文件偏移量
         * @param consumeFileId 归档消费文件ID
         * @param consumeOffset 归档消费文件偏移量
         * @param sync          是否要同步刷盘
         * @throws IOException
         */
        public void write(final long produceFileId, final long produceOffset, final long consumeFileId,
                          final long consumeOffset, boolean sync) throws IOException {
            getWriteLock().lock();
            try {
                if (!isStarted()) {
                    return;
                }
                setProduceFileId(produceFileId);
                setProduceOffset(produceOffset);
                setConsumeFileId(consumeFileId);
                setConsumeOffset(consumeOffset);
                flush(sync);
            } finally {
                getWriteLock().unlock();
            }
        }

        /**
         * 刷盘
         *
         * @throws IOException
         */
        protected void flush(final boolean sync) throws IOException {
            statRaf.seek(0);
            checksum = hashCode();
            for (int i = 0; i < 2; i++) {
                statRaf.writeLong(produceFileId);
                statRaf.writeLong(produceOffset);
                statRaf.writeLong(consumeFileId);
                statRaf.writeLong(consumeOffset);
                statRaf.writeLong(hashCode());
            }
            if (sync) {
                statRaf.getFD().sync();
            }
        }

    }

    /**
     * 文件操作类，支持追加写，随机写，随机读
     */
    protected class ArchiveFileAppender extends Service {
        private File file;
        private RandomAccessFile raf;
        private long offset;
        private File path;
        private long fileId;
        private long flushTime;

        public ArchiveFileAppender(File path, long fileId, long offset) {
            this.path = path;
            this.fileId = fileId;
            this.offset = offset;
            this.file = new File(path, fileId + TMP_FILE_SUFFIX);
        }

        public long getFlushTime() {
            return flushTime;
        }

        public void setFlushTime(long flushTime) {
            this.flushTime = flushTime;
        }

        public long getFileId() {
            return fileId;
        }

        public long getOffset() {
            return offset;
        }

        @Override
        protected void doStart() throws Exception {
            if (!Files.createFile(file)) {
                throw new IOException("create file error." + file.getPath());
            }
            raf = new RandomAccessFile(file, "rw");
            // 先要seek操作，否则将要覆盖原有文件。
            raf.seek(offset);
            super.doStart();
        }

        @Override
        protected void doStop() {
            try {
                if (raf != null) {
                    raf.getFD().sync();
                }
            } catch (IOException e) {
                logger.error("sync archive file error." + file.getPath());
            }
            Close.close(raf);
            raf = null;
            super.doStop();
        }


        /**
         * 追加写
         *
         * @param buffer Heap缓冲区
         * @throws IOException
         */
        public void append(final ByteBuffer buffer) throws IOException {
            if (buffer == null) {
                return;
            }
            getWriteLock().lock();
            try {
                if (!isStarted()) {
                    return;
                }
                buffer.flip();
                int length = buffer.remaining();
                raf.write(buffer.array(), buffer.arrayOffset() + buffer.position(), length);
                buffer.clear();
                this.offset = offset + length;
            } finally {
                getWriteLock().unlock();
            }
        }

        /**
         * 刷盘并产生新文件
         */
        public void flushAndRolling() {
            getWriteLock().lock();
            try {
                if (!isStarted()) {
                    return;
                }
                try {
                    // 写盘，关闭文件
                    raf.getFD().sync();
                    Close.close(raf);
                    raf = null;

                    // 移动文件
                    File target = new File(file.getParentFile(), fileId + LOG_FILE_SUFFIX);
                    Files.move(file, target);
                } catch (IOException e) {
                    // 忽略错误
                    logger.error("flush archive file error." + file.getPath());
                }

                // 产生新文件
                ++fileId;
                offset = 0;
                file = new File(path, fileId + TMP_FILE_SUFFIX);
                Files.createFile(file);
                raf = null;
                raf = new RandomAccessFile(file, "rw");
                flushTime = SystemClock.now();
            } catch (IOException e) {
                logger.error("create archive file error." + file.getPath());
            } finally {
                getWriteLock().unlock();
            }
        }

    }

    /**
     * 归档文件
     */
    protected class ArchiveFile {
        // 文件
        private File file;
        // 文件ID
        private long fileId;
        // 扩展名
        private String extension;
        // 长度
        private long length;

        public ArchiveFile(File file, long fileId, String extension, long length) {
            this.file = file;
            this.fileId = fileId;
            this.extension = extension;
            this.length = length;
        }

        public File getFile() {
            return file;
        }

        public void setFile(File file) {
            this.file = file;
        }

        public long getFileId() {
            return fileId;
        }

        public void setFileId(long fileId) {
            this.fileId = fileId;
        }

        public String getExtension() {
            return extension;
        }

        public void setExtension(String extension) {
            this.extension = extension;
        }

        public long getLength() {
            return length;
        }

        public void setLength(long length) {
            this.length = length;
        }
    }
}

