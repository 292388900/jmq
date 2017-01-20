package com.ipd.jmq.server.store;

import com.ipd.jmq.common.exception.ChecksumException;
import com.ipd.jmq.common.exception.JMQQueueException;
import com.ipd.jmq.common.message.QueueItem;
import com.ipd.jmq.server.store.journal.AppendDirectory;
import com.ipd.jmq.server.store.journal.AppendFile;
import com.ipd.jmq.server.store.journal.AppendFileManager;
import com.ipd.jmq.toolkit.buffer.RByteBuffer;
import com.ipd.jmq.toolkit.io.Files;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 消费队列，必须单线程写入数据
 */
@NotThreadSafe
public class ConsumeQueue extends Service {
    // 消费队列数据记录大小
    public static final int CQ_RECORD_SIZE = 8 + 4 + 2 + 8;
    private static Logger logger = LoggerFactory.getLogger(ConsumeQueue.class);
    private ByteBuffer shareBuffer = ByteBuffer.allocate(CQ_RECORD_SIZE);
    // 存储配置
    private StoreConfig config;
    // 主题
    private String topic;
    // 队列ID
    private int queueId;
    // 数据目录
    private File directory;
    // 追加文件操作
    private AppendDirectory appendDirectory;
    // 追加文件管理器
    private AppendFileManager appendFileManager;
    // 全局偏移量
    private long offset = 0;
    // 刷盘策略
    private FlushPolicy flushPolicy;
    // 刷盘条件
    private FlushPolicy.FlushCondition queueFlush = new FlushPolicy.FlushCondition();
    // 初始文件偏移量
    private long initOffset = 0;

    public ConsumeQueue(String topic, int queueId) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic can not be null");
        }
        this.topic = topic;
        this.queueId = queueId;
    }

    /**
     * 构造函数
     *
     * @param topic             主题
     * @param queueId           队列ID
     * @param directory         数据目录
     * @param appendFileManager 文件管理器
     * @param config            刷盘策略
     */
    public ConsumeQueue(String topic, int queueId, File directory, AppendFileManager appendFileManager,
                        StoreConfig config) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic can not be null");
        }
        if (directory == null) {
            throw new IllegalArgumentException("directory can not be null");
        }
        if (appendFileManager == null) {
            throw new IllegalArgumentException("appendFileManager can not be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("storeConfig can not be null");
        }
        this.topic = topic;
        this.queueId = queueId;
        this.directory = directory;
        this.appendFileManager = appendFileManager;
        this.flushPolicy = config.getQueueFlushPolicy();
        this.config = config;
    }

    public ConsumeQueue(String topic, int queueId, File directory, AppendFileManager appendFileManager,
                        StoreConfig config, long initOffset){
        this(topic, queueId, directory, appendFileManager, config);
        this.initOffset = initOffset;
    }

    @Override
    protected void validate() throws Exception {
        super.validate();
        Files.createDirectory(directory);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        // 获取追加目录对象
        appendDirectory = appendFileManager.getDirectory(directory);
        // 创建初始化文件
        appendDirectory.getAndCreate(initOffset);
        queueFlush.reset(SystemClock.getInstance().now());
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("consume queue is started,topic:%s,queueId:%d", topic, queueId));
        }
    }

    @Override
    protected void doStop() {
        // 刷盘
        try {
            flush();
        } catch (IOException e) {
            logger.error(String.format("flush consume queue error,topic:%s,queueId:%d", topic, queueId), e);
        }
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("consume queue is stopped,topic:%s,queueId:%d", topic, queueId));
        }
    }

    /**
     * 恢复
     *
     * @param recoverFiles 恢复的文件数量
     * @return 开始重建队列的日志位置
     * <li>&lt;0 不需要重建队列</li>
     * <li>&gt;=0 重建队列的日志位置</li>
     * @throws IOException
     */
    protected long recover(final int recoverFiles) throws IOException {
        int count = (recoverFiles <= 0 ? config.getRecoverQueueFiles() : recoverFiles);
        // 获取队列文件列表,升序排序
        List<AppendFile> files = appendDirectory.files();
        int fileCount = files == null ? 0 : files.size();
        logger.info(String.format("recover consume queue,topic:%s,queueId:%d,files:%d,recovers:%d", topic, queueId,
                fileCount, count));
        if (fileCount == 0) {
            return -1;
        }
        // 计算待恢复文件起始位置
        int fileIndex = fileCount - count;
        if (fileIndex < 0) {
            fileIndex = 0;
        }

        long journalOffset = 0;
        boolean hasErr = false;
        AppendFile file = null;
        RByteBuffer refBuf = null;
        QueueItem queueItem = new QueueItem();
        // 不包含文件头
        int filePos = 0;
        int blockSize;
        int blockPos;
        long queueOffset = 0;
        while (isStarted() && fileIndex < fileCount) {
            // 是否需要恢复下一个文件
            if (file == null) {
                // 获取下一个文件
                file = files.get(fileIndex);
                filePos = 0;
                queueOffset = file.id();
              /* if (fileIndex == 0) {
                    filePos += file.headSize();
                    queueOffset += file.headSize();
                }*/
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("recover consume queue,topic:%s,queueId:%d,file:%s", topic, queueId,
                            file.file().getName()));
                }
            }
            // 获取数据缓冲区
            try {
                ByteBuffer buf = null;
                if (filePos < config.getBlockSize()){
                    refBuf = new RByteBuffer(file.read(filePos, config.getBlockSize()), file);
                    buf = refBuf.getBuffer();
                }
                //TODO 恢复时处理迁移过来的文件
                if (buf == null || buf.remaining() < CQ_RECORD_SIZE) {
                    // 文件结尾，准备切换到下一个文件
                    file = null;
                    fileIndex++;
                    continue;
                }
                blockSize = buf.remaining();
                blockPos = 0;
                // 循环块数据
                while (isStarted() && blockPos < blockSize) {
                    if (blockPos + CQ_RECORD_SIZE > blockSize) {
                        // 块数据空间不够了
                        if (filePos + CQ_RECORD_SIZE > file.length()) {
                            // 文件末尾，切换到下一个文件
                            fileIndex++;
                            file = null;
                        }
                        // 重新读取后续数据
                        break;
                    }
                    // 读取队列元素
                    try {
                        queueItem.clear();
                        queueItem.setQueueOffset(file.id() + filePos);
                        queueItem.decode(buf, config.isChecksumOnRecover());
                        // 有效数据，切换到下一条数据
                        journalOffset = queueItem.getJournalOffset() + queueItem.getSize();
                        blockPos += CQ_RECORD_SIZE;
                        filePos += CQ_RECORD_SIZE;
                        queueOffset += CQ_RECORD_SIZE;
                    } catch (ChecksumException e) {
                        // 校验和出错
                        if (queueItem.getSize() == 0 && queueItem.getJournalOffset() == 0 && queueItem
                                .getCrc() == 0 && fileIndex == fileCount - 1) {
                            // 文件末尾空洞
                            fileIndex++;
                            break;
                        }
                        hasErr = true;
                        // 校验和问题
                        logger.error(
                                String.format("recover consume queue error,topic:%s,queueId:%d,queueOffset:%d", topic,
                                        queueId, queueItem.getQueueOffset()), e);
                        break;
                    }
                }
                if (hasErr) {
                    break;
                }
            } finally {
                if (refBuf != null) {
                    refBuf.release();
                    refBuf = null;
                }
            }
        }
        if (!hasErr) {
            // 没有异常则不需要重建索引
            journalOffset = -1;
        }
        // 裁剪无效的数据
        if (isStarted()) {
            truncate(queueOffset);
        }else{
            logger.warn(String.format("service is stopping,topic:%s,queueId:%d",topic,queueId));
        }
        return journalOffset;
    }

    /**
     * 删除指定位置之后的数据
     *
     * @param offset 队列位置
     * @throws IOException
     */
    protected void truncate(final long offset) throws IOException {
        appendDirectory.truncate(offset);
        this.offset = offset;
    }

    /**
     * 删除指定位置之前的数据
     *
     * @param queueOffset         日志位置
     * @param retentionTime       文件保留时间(毫秒)
     * @param deleteFilesInterval 删除多个文件时间间隔(毫秒)
     * @throws IOException
     */
    public void truncateBefore(final long journalOffset, final long queueOffset, final long retentionTime,
            final int deleteFilesInterval) throws IOException {
        boolean flag;
        while (isStarted()) {
            AppendFile file = appendDirectory.first();
            if (file == null) {
                //清空全部文件时可能发生
                return;
            }
            int lastPos = (int) ((file.length() / CQ_RECORD_SIZE) - 1) * CQ_RECORD_SIZE;
            if (file.writePosition() <= lastPos) {
                //日志没写完
                if(logger.isTraceEnabled()){
                    logger.trace(String.format("is in write,topic:%s,queueId:%d,file:%d,writePos:%d",topic,queueId,file.id(),file.writePosition()));
                }
                return;
            }

            List<QueueItem> list = null;
            try {
                list = this.getQueueItem(file.id() + lastPos, 1);
            } catch (IOException e) {
                logger.error(file.file().getPath() + " lastPos:" + lastPos, e);
            }
            if (list != null && !list.isEmpty()) {
                QueueItem item = list.get(0);
                if (item.getJournalOffset() >= journalOffset) {
                    //有日志被引用
                    if(logger.isTraceEnabled()){
                        logger.trace(String.format("journal is not clean,topic:%s,queueId:%d,file:%d,minJournalOffset:%d",topic,queueId,file.id(),item.getJournalOffset()));
                    }
                    return;
                }
            } else {
                return;
            }

            flag = appendDirectory.deleteOldest(queueOffset, retentionTime);
            if (!flag) {
                break;
            }
            if (deleteFilesInterval > 0) {
                await(deleteFilesInterval);
            }
        }
    }

    /**
     * 删除指定位置之前的数据
     *
     * @param deleteFilesInterval 删除多个文件时间间隔(毫秒)
     * @throws IOException
     */
    public void truncateAll(final int deleteFilesInterval) throws IOException {
        boolean flag;
        long end = this.getMaxOffset() + config.getQueueConfig().getDataSize();
        while (isStarted()) {
            flag = appendDirectory.deleteOldest(end, 0);
            if (!flag) {
                break;
            }
            if (deleteFilesInterval > 0) {
                await(deleteFilesInterval);
            }
        }
    }

    /**
     * 裁剪指定日志位置之后后队列条目
     *
     * @param journalOffset 日志文件偏移量
     * @throws IOException
     */
    public void truncateByJournalOffset(final long journalOffset) throws IOException {
        if (journalOffset <= 0) {
            logger.info(String.format("truncate consume queue,topic:%s,queueId:%d,offset:%d", topic, queueId, 0));
            truncate(0);
            return;
        }
        // 得到最后一个文件
        AppendFile file = appendDirectory.last();

        int pos;
        int writePosition;
        QueueItem item = new QueueItem();
        long minQueueOffset = -1;
        // 从后往前遍历
        while (isStarted() && file != null) {
            // 判断当前文件是否有数据
            writePosition = file.writePosition();
            if (writePosition < CQ_RECORD_SIZE) {
                // 没有数据切换到前一个文件
                file = file.id() == 0 ? null : appendDirectory.get(file.id() - 1);
            } else {
                // 计算最后位置，从后往前开始计算的相对位置
                pos = (writePosition - writePosition % CQ_RECORD_SIZE) - CQ_RECORD_SIZE;
                // 遍历该文件
                while (isStarted() && pos >= 0) {
                    // 获取队列条目
                    RByteBuffer refBuf = null;

                    try {
                        refBuf = new RByteBuffer(file.read(pos, CQ_RECORD_SIZE), file);
                        item.decode(refBuf.getBuffer(), false);
                    } finally {
                        if (refBuf != null) {
                            refBuf.release();
                            refBuf = null;
                        }
                    }
                    item.setQueueOffset(file.id() + pos);
                    if (item.getJournalOffset() <= journalOffset) {
                        // 日志文件位置在指定位置之前
                        break;
                    } else {
                        // 记录最小的队列偏移量
                        minQueueOffset = item.getQueueOffset();
                    }
                    item.clear();
                    pos -= CQ_RECORD_SIZE;
                }
                // 该文件遍历完
                if (minQueueOffset < 0) {
                    // 该文件没有需要裁剪的数据
                    break;
                } else if (minQueueOffset > file.id()) {
                    // 该文件有部分需要裁剪，或者后面的文件需要裁剪
                    logger.info("truncate queue topic:%s,queueId:%d,offset:%d", topic, queueId, minQueueOffset);
                    truncate(minQueueOffset);
                    break;
                } else if (minQueueOffset == file.id()) {
                    // 该文件全部裁剪，切换到前一个文件
                    file = file.id() == 0 ? null : appendDirectory.get(file.id() - 1);
                    if (file == null) {
                        // 前一个文件不存在
                        truncate(minQueueOffset);
                    }
                }
            }
        }
    }


    /**
     * 删除所有文件
     *
     * @throws IOException
     */
    public void deleteAllFile(final int deleteFilesInterval) throws IOException {
        boolean flag;
        while (isStarted()) {
            AppendFile af = appendDirectory.last();
            if (af == null) {
                break;
            }
            flag = appendDirectory.deleteOldest(af.id() + af.maxDataSize(), 0);
            if (!flag) {
                break;
            }
            if (deleteFilesInterval > 0) {
                await(deleteFilesInterval);
            }
        }
    }

    /**
     * 写入数据，使用了共享缓冲区，必须单线程写入
     *
     * @param request 写入请求
     * @throws IOException
     */
    public void append(DispatchRequest request) throws Exception {
        if (request == null) {
            return;
        }
        // 文件大小
        int fileLength = config.getQueueConfig().getDataSize();
        // 编码数据
        shareBuffer.clear();
        request.encode(shareBuffer);
        shareBuffer.flip();

        AppendFile file;
        if (request.isRetry()) {
            // 恢复重新写入数据
            file = appendDirectory.getAndCreatePrev(request.getQueueOffset(), true);
            // 最后写入位置
            int writePos = file.writePosition();
            int pos = (int) (request.getQueueOffset() - file.id());
            if (pos > writePos && writePos != file.headSize()) { //如果pos大于当前文件写的位置，并且当前文件写位置不在开始位置
                if (pos == file.headSize()) {
                    logger.warn("first file[" + file.file().getPath() + "] start:" + file.header());
                } else {
                    //文件出现空洞
                    throw new JMQQueueException(String.format(
                            "retry write position error exteed last position. topic:%s,queueId=%d,file:%s," +
                                    "queueOffset:%d,writePos:%d," +
                                    "" + "journalOffset:%d,messageSize:%d", topic, queueId, file.file().getName(),
                            request.getQueueOffset(), writePos, request.getJournalOffset(), request.getSize()));
                }
            }
            file.write(shareBuffer, pos);
        } else {
            // 获取最后的文件
            file = appendDirectory.lastAndCreate();
            // 最后写入位置
            int writePos = file.writePosition();
            // 偏移量
            int pos = (int) (request.getQueueOffset() - file.id());

            // 超过当前文件
            if (writePos + shareBuffer.remaining() > fileLength) {
                // 当前文件刷盘
                queueFlush.reset(SystemClock.getInstance().now());
                file.flush();
                // 创建新文件
                file = appendDirectory.create();
                // 得到当前文件写入位置
                writePos = file.writePosition();
                pos = (int) (request.getQueueOffset() - file.id());
            }

            int len = shareBuffer.remaining();

            // 写入位置错误
            if (writePos != pos && writePos != file.headSize()) {
                throw new IOException(String.format(
                        "write position error. topic:%s,queueId=%d,file:%s,queueOffset:%d,writePos:%d,size:%d," +
                                "" + "journalOffset:%d,messageSize:%d", topic, queueId, file.file().getName(),
                        request.getQueueOffset(), writePos, len, request.getJournalOffset(), request.getSize()));
            }

            // 追加写入文件
            file.write(shareBuffer, pos);
        }

        // 写入数据量
        queueFlush.addSize(CQ_RECORD_SIZE);
        if (queueFlush.match(flushPolicy, SystemClock.now(), true)) {
            file.flush();
        }
    }

    private void appendMigratedMessage(){

    }

    /**
     * 读取队列中的数据
     *
     * @param offset 全局偏移量
     * @param count  条数
     * @return 队列条目列表
     * @throws IOException
     */
    public List<QueueItem> getQueueItem(final long offset, final int count) throws IOException {
        int total = count > 0 ? count : 1;
        List<QueueItem> items = new ArrayList<QueueItem>(total);
        // 获取文件
        AppendFile file = appendDirectory.get(offset);
        if (file == null) {
            return items;
        }
        // 当前写入数据的位置
        int writePosition = file.writePosition();
        // 文件中相对偏移量
        int pos = (int) (offset - file.id());
        // 校对好索引的位置
//        if (pos % CQ_RECORD_SIZE > 0) {
//            pos = (pos / CQ_RECORD_SIZE + 1) * CQ_RECORD_SIZE;
//        }
        int available;
        int reads;
        RByteBuffer refBuf = null;
        long queueOffset;
        QueueItem item;
        while (total > 0) {
            // 当前文件可以读取的数据条数
            available = (writePosition - pos) / CQ_RECORD_SIZE;
            // 实际读取条数
            reads = available >= total ? total : available;
            // 读取数据
            if (reads > 0) {
                try {
                    refBuf = new RByteBuffer(file.read(pos, CQ_RECORD_SIZE * reads), file);
                    queueOffset = file.id() + pos;
                    ByteBuffer realBuf = refBuf.getBuffer();
                    while (realBuf.hasRemaining()) {
                        item = new QueueItem();
                        item.setQueueOffset(queueOffset);
                        item.setTopic(topic);
                        item.setQueueId((short)queueId);
                        item.decode(realBuf, false);
                        items.add(item);
                        total--;
                        queueOffset += CQ_RECORD_SIZE;
                    }
                } finally {
                    if (refBuf != null) {
                        refBuf.release();
                        refBuf = null;
                    }
                }
            }
            // 还有剩余条数
            if (total > 0) {
                // 切换到到下一个文件
                file = appendDirectory.get(file.id() + file.maxDataSize());
                if (file != null) {
                    writePosition = file.writePosition();
                    pos = 0;
                } else {
                    total = 0;
                }
            }
        }
        return items;
    }

    /**
     * 刷盘
     *
     * @throws IOException
     */
    public void flush() throws IOException {
        // 只需要刷最后的文件，切换文件的时候会自动刷盘
        AppendFile af = appendDirectory.last();
        if (af != null) {
            queueFlush.reset(SystemClock.getInstance().now());
            af.flush();
        }
    }

    /**
     * 获取偏移量
     *
     * @return 偏移量
     */
    public long getOffset() {
        return offset;
    }

    /**
     * 设置偏移量
     *
     * @param offset 偏移量
     */
    public void setOffset(long offset) {
        this.offset = offset;
    }

    /**
     * 获取最大队列偏移量
     *
     * @return 最大队列偏移量
     */
    public long getMaxOffset() {
        return appendDirectory.writeOffset();
    }

    /**
     * 获取刷盘位置
     *
     * @return 刷盘位置
     */
    public long getFlushOffset() {
        return 0;//// TODO: 2016/7/28
        //return appendDirectory.getFlushOffset();
    }

    /**
     * 获取最小位置
     *
     * @return 最小位置
     */
    public long getMinOffset() {
        return 0;//// TODO: 2016/7/28
        //return appendDirectory.getMinActiveOffset();
    }

    /**
     * 获取下一个位置
     *
     * @param offset 当前位置
     * @return 下一个位置
     */
    public long getNextOffset(final long offset) {
        return offset + CQ_RECORD_SIZE;
    }

    /**
     * 准备下一个写入的索引位置
     */
    public void updateOffset() {
        offset += CQ_RECORD_SIZE;
    }

    /**
     * 更新需要写入的索引位置
     * @param count
     */
    public void updateOffset(int count) {
        long off = count * CQ_RECORD_SIZE;
        offset += off;
    }

    public AppendDirectory getAppendDirectory() {
        return appendDirectory;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConsumeQueue that = (ConsumeQueue) o;

        if (queueId != that.queueId) {
            return false;
        }
        if (!directory.equals(that.directory)) {
            return false;
        }
        if (!topic.equals(that.topic)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + (int) queueId;
        result = 31 * result + directory.hashCode();
        return result;
    }
}