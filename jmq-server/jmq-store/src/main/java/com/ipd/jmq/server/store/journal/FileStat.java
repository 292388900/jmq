package com.ipd.jmq.server.store.journal;

import com.ipd.jmq.toolkit.stat.TPStatBuffer;
import com.ipd.jmq.toolkit.stat.TPStatDoubleBuffer;
import com.ipd.jmq.toolkit.stat.TPStatSlice;
import com.ipd.jmq.toolkit.time.MicroPeriod;
import com.ipd.jmq.toolkit.time.Period;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 文件统计
 */
public class FileStat extends TPStatDoubleBuffer<FileStat.FileStatSlice> {
    public static final int ONE_MINUTE = 1000 * 60;
    // 内存镜像缓冲区大小
    protected AtomicLong mappedMemory = new AtomicLong(0);
    // 内存镜像文件数量
    protected AtomicInteger mappedFiles = new AtomicInteger(0);
    // 追加文件数据大小（不包括头部长度）
    protected AtomicLong appendFileLength = new AtomicLong(0);
    // 追加文件数量
    protected AtomicInteger appendFiles = new AtomicInteger(0);
    // 是否开启性能统计
    protected boolean perfStat;

    public FileStat() {
        this(true, ONE_MINUTE);
    }

    public FileStat(final boolean perfStat) {
        this(perfStat, ONE_MINUTE);
    }

    public FileStat(final boolean perfStat, final long interval) {
        super(null, null, interval);
        this.perfStat = perfStat;
        if (perfStat) {
            writeStat = new FileStatSlice();
            readStat = new FileStatSlice();
        }
    }

    /**
     * 获取内存镜像文件数
     *
     * @return 内存镜像文件数
     */
    public int getMappedFiles() {
        return this.mappedFiles.get();
    }

    /**
     * 获取内存镜像缓冲区数据大小
     *
     * @return 内存镜像缓冲区数据大小(不包括文件头长度)
     */
    public long getMappedMemory() {
        return this.mappedMemory.get();
    }

    /**
     * 获取追加文件大小
     *
     * @return 追加文件大小
     */
    public long getAppendFileLength() {
        return appendFileLength.get();
    }

    /**
     * 获取追加文件数
     *
     * @return 追加文件数
     */
    public int getAppendFiles() {
        return appendFiles.get();
    }

    /**
     * 清理
     */
    public void clear() {
        appendFiles.set(0);
        appendFileLength.set(0);
        mappedFiles.set(0);
        mappedMemory.set(0);
    }

    /**
     * 成功写入
     *
     * @param count 记录调试
     * @param size  大小
     * @param time  时间
     */
    public void success(final int count, final long size, final int time) {
        if (perfStat) {
            lock.readLock().lock();
            try {
                writeStat.getWriteStat().success(count, size, time);
            } finally {
                lock.readLock().unlock();
            }
        }
    }

    /**
     * 错误
     */
    public void error() {
        if (perfStat) {
            lock.readLock().lock();
            try {
                writeStat.getWriteStat().error();
            } finally {
                lock.readLock().unlock();
            }
        }
    }

    /**
     * 性能统计
     */
    public static class FileStatSlice implements TPStatSlice {
        // 写入性能
        protected TPStatBuffer writeStat = new TPStatBuffer();
        // 时间片段
        protected MicroPeriod period = new MicroPeriod();

        @Override
        public Period getPeriod() {
            return period;
        }

        public TPStatBuffer getWriteStat() {
            return writeStat;
        }

        @Override
        public void clear() {
            writeStat.clear();
            period.clear();
        }

    }

}

