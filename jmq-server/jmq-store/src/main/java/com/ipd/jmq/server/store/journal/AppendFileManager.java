package com.ipd.jmq.server.store.journal;

import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 追加文件目录管理器
 */
public class AppendFileManager extends Service {
    protected static Logger logger = LoggerFactory.getLogger(AppendFileManager.class);
    // 配置
    protected JournalConfig config;
    // 文件头
    protected FileHeader header;
    // 内存镜像文件管理
    protected MappedFileManager cacheManager;
    // 文件统计
    protected FileStat fileStat = new FileStat();
    // 目录管理
    protected ConcurrentMap<File, AppendDirectory> directories = new ConcurrentHashMap<File, AppendDirectory>();

    /**
     * 构造函数
     *
     * @param config 内存映射配置，日志和队列的配置分开，便于控制热点参数
     * @param header 头部
     */
    public AppendFileManager(JournalConfig config, FileHeader header) {
        this(config, header, null);
    }

    /**
     * 构造函数
     *
     * @param config    内存映射配置，日志和队列的配置分开，便于控制热点参数
     * @param header    头部
     * @param cacheName 缓存名称，如果为空则不启用缓存
     */
    public AppendFileManager(JournalConfig config, FileHeader header, String cacheName) {
        Preconditions.checkArgument(config != null, "config can not be null");
        Preconditions.checkArgument(header != null, "header can not be null");

        this.config = config;
        this.header = header;
        if (cacheName != null) {
            this.cacheManager = new MappedFileManager(config, fileStat, cacheName);
        }
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        if (cacheManager != null) {
            cacheManager.start();
        }
        logger.info("AppendFileManager is started.");
    }

    @Override
    protected void doStop() {
        for (AppendDirectory directory : directories.values()) {
            directory.stop();
        }
        directories.clear();
        Close.close(cacheManager);
        fileStat.clear();
        super.doStop();
        logger.info("AppendFileManager is stopped.");
    }

    /**
     * 获取追加目录
     *
     * @param directory 目录
     * @throws IOException
     */
    public AppendDirectory getDirectory(final File directory) throws Exception {
        Preconditions.checkArgument(directory != null, "directory can not be null");

        readLock.lock();
        try {
            if (!isStarted()) {
                throw JournalException.IllegalStateException.build("AppendFileManager is not started.");
            }
            AppendDirectory dir = directories.get(directory);
            if (dir == null) {
                dir = new AppendDirectory(directory, config, header, fileStat, cacheManager);
                AppendDirectory old = directories.putIfAbsent(directory, dir);
                if (old != null) {
                    dir = old;
                } else {
                    dir.start();
                }
            }
            return dir;
        } finally {
            readLock.unlock();
        }
    }

    public FileStat getFileStat() {
        return fileStat;
    }

}