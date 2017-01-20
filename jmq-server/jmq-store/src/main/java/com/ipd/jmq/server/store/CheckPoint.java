package com.ipd.jmq.server.store;

import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.toolkit.io.Files;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * 检查点文件，双写
 */
public class CheckPoint extends Service {
    // 检查点数据大小
    private static final int SIZE = 8 + 8 + 8 + 4;
    // 第二份数据的位置
    private static final int NEXT = 1024;
    // 日志写入偏移量
    protected long journalOffset;
    // 日志恢复起始偏移量
    protected long recoverOffset;
    // 时间戳
    protected long timestamp;
    // 文件
    protected File file;
    protected RandomAccessFile raf;
    private static Logger logger = LoggerFactory.getLogger(CheckPoint.class);

    /**
     * 构造函数
     *
     * @param file 本地存储文件
     */
    public CheckPoint(File file) {
        if (file == null) {
            throw new IllegalArgumentException("file can not be null");
        }
        this.file = file;
    }

    @Override
    protected void validate() throws Exception {
        super.validate();
        if (!Files.createFile(file)) {
            throw new IOException(String.format("create file error,%s", file.getPath()));
        }
        if (!file.canWrite()) {
            throw new IOException(String.format("file can not be written,%s", file.getPath()));
        }
        if (!file.canRead()) {
            throw new IOException(String.format("file can not be read,%s", file.getPath()));
        }
        if (raf == null) {
            raf = new RandomAccessFile(file, "rw");
        }
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        recover();
        logger.info("checkpoint is started.");
    }

    @Override
    protected void doStop() {
        doFlush();
        Close.close(raf);
        raf = null;
        super.doStop();
        logger.info("checkpoint is stopped.");
    }

    /**
     * 恢复
     *
     * @throws Exception
     */
    protected void recover() throws Exception {
        long length = raf.length();
        if (length >= SIZE) {
            // 读取第一份数据
            raf.seek(0);
            recoverOffset = raf.readLong();
            journalOffset = raf.readLong();
            timestamp = raf.readLong();
            int checksum = raf.readInt();
            int value = getCheckSum(journalOffset, recoverOffset, timestamp);
            // 校验和不同
            if (checksum != value) {
                // 尝试取第二份数据
                if (length >= NEXT + SIZE) {
                    // 第一份数据校验和错误，读取出错
                    raf.seek(NEXT);
                    recoverOffset = raf.readLong();
                    journalOffset = raf.readLong();
                    timestamp = raf.readLong();
                    checksum = raf.readInt();
                    value = getCheckSum(journalOffset, recoverOffset, timestamp);
                    if (checksum != value) {
                        // 致命异常
                        throw new JMQException(JMQCode.SE_FATAL_ERROR);
                    }
                } else {
                    throw new JMQException(JMQCode.SE_FATAL_ERROR);
                }

            }
            logger.info(
                    String.format("recover checkpoint. recoverOffset:%d,journalOffset:%d,timestamp:%d", recoverOffset,
                            journalOffset, timestamp));
        }
    }

    /**
     * 写入到磁盘中
     */
    protected void doFlush() {
        long timestamp = SystemClock.now();
        long recoverOffset = this.recoverOffset;
        long journalOffset = this.journalOffset;
        int checksum = getCheckSum(journalOffset, recoverOffset, timestamp);
        try {
            // 双写
            raf.seek(0);
            raf.writeLong(recoverOffset);
            raf.writeLong(journalOffset);
            raf.writeLong(timestamp);
            raf.writeInt(checksum);

            raf.seek(NEXT);
            raf.writeLong(recoverOffset);
            raf.writeLong(journalOffset);
            raf.writeLong(timestamp);
            raf.writeInt(checksum);

            raf.getFD().sync();
        } catch (IOException e) {
            logger.error("flush checkpoint error.", e);
        } finally {
            this.timestamp = timestamp;
        }
    }

    /**
     * 刷盘
     */
    public void flush() {
        writeLock.lock();
        try {
            if (!isStarted()) {
                return;
            }
            doFlush();
        } finally {
            writeLock.unlock();
        }
    }

    public long getJournalOffset() {
        return journalOffset;
    }

    public void setJournalOffset(long journalOffset) {
        this.journalOffset = journalOffset;
    }

    public long getRecoverOffset() {
        return recoverOffset;
    }

    public void setRecoverOffset(long recoverOffset) {
        this.recoverOffset = recoverOffset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    /**
     * 获取校验和
     *
     * @param journalOffset 日志偏移量
     * @param recoverOffset 恢复偏移量
     * @param timestamp     时间戳
     * @return 校验和
     */
    protected int getCheckSum(final long journalOffset, final long recoverOffset, final long timestamp) {
        int result = (int) (journalOffset ^ (journalOffset >>> 32));
        result = 31 * result + (int) (recoverOffset ^ (recoverOffset >>> 32));
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }
}