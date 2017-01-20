package com.ipd.jmq.server.store.journal;

import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.service.ServiceThread;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 内存镜像文件管理
 */
public class MappedFileManager extends Service {
    public static final int FILE_LOCK_TIMEOUT = 20;
    protected static Logger logger = LoggerFactory.getLogger(MappedFileManager.class);
    /**
     * 在锁中
     */
    protected static final int LOCK_IN = 2;
    /**
     * 加锁成功
     */
    protected static final int LOCK_SUCCESS = 1;
    /**
     * 加锁失败
     */
    protected static final int LOCK_FAIL = 0;

    // 存储配置
    protected JournalConfig config;
    // 文件统计信息
    protected FileStat fileStat;
    // 文件映射
    protected ConcurrentMap<AppendFile, MappedFile> mappedFiles = new ConcurrentHashMap<AppendFile, MappedFile>();
    // 待释放文件
    protected ConcurrentMap<AppendFile, Long> releaseFiles = new ConcurrentHashMap<AppendFile, Long>();
    // 等待清理线程退出
    protected CountDownLatch deathLatch;
    // 清理线程
    protected Thread cleaner;
    // 线程名称
    protected String name;
    // 致命的异常，自我保护
    protected JournalException.FatalException fatal;

    public MappedFileManager(JournalConfig config, FileStat fileStat, String name) {
        Preconditions.checkArgument(config != null, "file config not be null");
        Preconditions.checkArgument(name != null || name.isEmpty(), "name can not be null");
        Preconditions.checkArgument(fileStat != null, "fileStat can not be null");

        this.config = config;
        this.fileStat = fileStat;
        this.name = name;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        deathLatch = new CountDownLatch(1);
        // 创建并启动清理线程
        cleaner = new Thread(new ServiceThread(this) {
            @Override
            protected void execute() throws Exception {
                // 执行清理检查
                cleanup();
            }

            @Override
            public long getInterval() {
                return config.getCacheCheckInterval();
            }

            @Override
            protected void stop() {
                if (deathLatch != null) {
                    deathLatch.countDown();
                }
            }

            @Override
            public boolean onException(Throwable e) {
                logger.error("clean mmap file error.", e);
                return true;
            }

        }, "clean-mmap-" + name);
        cleaner.start();
        logger.info(name + " MappedFileManager is started");
    }

    @Override
    protected void doStop() {
        // 唤醒休息
        if (cleaner != null) {
            cleaner.interrupt();
        }
        // 等待清理线程结束
        if (deathLatch != null) {
            try {
                deathLatch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
        }

        // 清理内存镜像文件
        for (Map.Entry<AppendFile, MappedFile> entry : mappedFiles.entrySet()) {
            unmapped(entry.getKey());
            close(entry.getValue());
        }
        mappedFiles.clear();
        releaseFiles.clear();
        fatal = null;
        logger.info(name + " mappedFileManager is stopped");
    }

    /**
     * 切换到文件通道
     *
     * @param file 追加文件
     */
    protected void unmapped(final AppendFile file) {
        // 还在被使用
        long references = file.references();
        if (references > 0) {
            logger.error(String.format("mapped file maybe memory leak,reference:%d,visited before %d(ms),file:%s ",
                    references, file.accessTime() - SystemClock.now(), file.file().getPath()));

        }
        try {
            file.unmapped();
        } catch (JournalException.FatalException e) {
            fatal = e;
            logger.error("unmapped file error. forbid reading, writing and creating. " + file.file().getPath(), e);
        } catch (JournalException.IllegalStateException ignored) {
        } catch (IOException e) {
            logger.error("unmapped file error. forbid reading and writing. " + file.file().getPath(), e);
        }
    }

    /**
     * 关闭内存镜像文件
     *
     * @param mappedFile
     */
    protected void close(final MappedFile mappedFile) {
        if (mappedFile == null) {
            return;
        }
        // TODO 卸载内存镜像 内存可能还在使用，存在非法访问内存的风险
        try {
            mappedFile.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 是否是热点文件
     *
     * @param file 文件
     * @return 热点文件标示
     */
    protected boolean isHotspot(final AppendFile file) {
        // 能缓存的最大文件数量
        int capacity = (int) ((config.getCacheSize() * config.getCacheMaxRatio() / 100) / file.maxDataSize());
        if (capacity <= 0) {
            return false;
        }
        // 获取目录文件列表
        List<AppendFile> files = file.directory().files();
        if (files.isEmpty()) {
            return true;
        }
        // 计算序号
        int index = 0;
        // 每个目录的热点文件数量
        int cacheFiles = config.getCacheFiles() > 0 ? config.getCacheFiles() : capacity;
        if (files.size() > cacheFiles) {
            index = files.size() - cacheFiles;
        }
        AppendFile appendFile = files.get(index);
        if (file.id() >= appendFile.id()) {
            return true;
        }
        return false;
    }

    /**
     * 锁定文件
     *
     * @param file 文件
     * @return 锁定标示
     */
    protected int tryLock(final AppendFile file) {
        switch (file.getServiceState()) {
            case STARTING:
                return LOCK_IN;
            case STOPPING:
                return LOCK_IN;
        }
        // 文件锁
        try {
            if (file.getWriteLock().tryLock(FILE_LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
                return LOCK_SUCCESS;
            }
            return LOCK_FAIL;
        } catch (InterruptedException ignored) {
            return LOCK_FAIL;
        }
    }

    /**
     * 是否能缓存
     *
     * @param file 文件
     * @return 能缓存标示
     */
    protected boolean cacheable(final AppendFile file) {
        long cacheSize = config.getCacheSize() * config.getCacheMaxRatio() / 100;
        if (cacheSize <= 0) {
            logger.info("cache is disable. use channelFile. " + file.file().getPath());
            return false;
        } else if (!isHotspot(file)) {
            // 是否优先缓存该文件
            logger.info("it is not hot file. use channelFile. " + file.file().getPath());
            return false;
        } else if (fileStat.getMappedMemory() >= cacheSize) {
            // 计算缓存的最大阀值，超过不能缓存（自我保护)
            // TODO 应该增加淘汰算法，把老的立即淘汰掉，缓存最新的数据
            logger.info("exceed max cache memory,use channelFile " + file.file().getPath());
            return false;
        }
        return true;
    }

    /**
     * 新文件创建时候，尝试缓存
     *
     * @param file 文件
     * @return 缓存成功标示
     * @throws IOException
     */
    protected MappedFile cache(final AppendFile file) throws IOException {
        if (file == null || config.getCacheMaxRatio() <= 0 || config.getCacheSize() <= 0) {
            return null;
        }
        MappedFile mf = null;
        // 文件锁标示
        int fileLock = LOCK_FAIL;
        // 使用写锁，防止并发创建缓存，内存计算会出问题
        writeLock.lock();
        try {
            if (!isStarted()) {
                throw JournalException.IllegalStateException.build(name + " mappedFileManager is stopped");
            } else if (fatal != null) {
                // 内存出了问题，自我保护，禁止创建新文件了
                throw JournalException.MappedFileException.build(fatal.getMessage());
            }
            // 判断是否已经缓存
            mf = mappedFiles.get(file);
            if (mf != null) {
                return null;
            } else if (!cacheable(file)) {
                return null;
            } else if ((fileLock = tryLock(file)) == LOCK_FAIL) {
                // 文件锁不成功
                logger.info("file lock timeout. use channelFile. " + file.file().getPath());
                return null;
            }

            // 构造内存镜像文件
            mf = new MappedFile(file.channelFile(), fileStat);
            // 缓存
            mappedFiles.put(file, mf);
            logger.info("memory is enough. use mappedFile. " + file.file().getPath());
            return mf;
        } catch (IOException e) {
            // 出了异常，清理内存，抛出异常
            close(mf);
            throw e;
        } finally {
            if (fileLock == LOCK_SUCCESS) {
                file.getWriteLock().unlock();
            }
            writeLock.unlock();
        }
    }

    /**
     * 释放内存镜像文件
     *
     * @param file    文件
     * @param latency 延迟标示
     * @return 成功标示
     */
    protected boolean release(final AppendFile file, final boolean latency) {
        if (file == null) {
            return false;
        }

        // 文件锁标示
        int fileLock = LOCK_FAIL;
        readLock.lock();
        try {
            if (!isStarted()) {
                return false;
            }
            // 获取内存镜像文件
            MappedFile mf = mappedFiles.get(file);
            if (mf == null) {
                return true;
            } else if ((fileLock = tryLock(file)) == LOCK_FAIL) {
                // 尝试文件锁
                logger.info("file lock timeout. break releasing the journal file. " + file.file().getPath());
                return false;
            }

            // 判断文件是否被使用
            long used = file.references();
            // 如果被使用，判断离上次使用的时间间隔是否超过阀值
            if (used > 0 && file.accessTime() > 0 && SystemClock.now() - file.accessTime() > config
                    .getCacheLeakTime()) {
                logger.warn(
                        String.format("mapped file maybe memory leak,reference %d,visited before %d(ms),file %s", used,
                                SystemClock.now() - file.accessTime(), file.file().getPath()));
                // 清理文件计数
                while (used > 0) {
                    file.release();
                    used = file.references();
                }
            }

            // 如果文件没有被使用，则卸载内存镜像文件
            if (used <= 0) {
                unmapped(file);
                if (latency && config.getCacheEvictLatency() > 0) {
                    // 延迟卸载，先放到待释放队列中，防止潜在的内存访问冲突
                    releaseFiles.put(file, SystemClock.now());
                    logger.info("put the mapped file to release queue. " + file.file().getPath());
                } else {
                    // 立即卸载
                    close(mf);
                    releaseFiles.remove(file);
                    mappedFiles.remove(file);
                    logger.info("the journal file is released. " + file.file().getPath());
                }
                return true;
            }
            return false;
        } finally {
            if (fileLock == LOCK_SUCCESS) {
                file.getWriteLock().unlock();
            }
            readLock.unlock();
        }
    }

    /**
     * 检查文件，判断是否需要进行清理
     */
    protected void cleanup() {
        AppendFile file;
        AppendDirectory directory;

        // 待释放的缓存数据大小(不包括文件头)
        long releasingMemory = releaseCaches();
        if (config.getCacheSize() <= 0) {
            return;
        }
        // 计算内存占用比率
        int ratio = (int) ((fileStat.getMappedMemory() - releasingMemory) * 100 / config.getCacheSize());
        if (ratio <= config.getCacheNewRatio()) {
            // 没有超过新生代比率，不需要进行清理
            return;
        }
        // 强制清理的内存
        long forceCleanup = 0;
        // 判断是否达到强制清理阀值
        if (ratio > config.getCacheEvictThreshold()) {
            // 需要强制清理的内存
            forceCleanup = config.getCacheSize() * config.getCacheEvictMinRatio() / 100 - releasingMemory;
        }

        // 文件优先级
        final Map<AppendFile, Integer> priorities = new HashMap<AppendFile, Integer>();
        // 文件索引
        Map<AppendFile, Integer> fileIndices = new HashMap<AppendFile, Integer>();
        // 已处理目录索引
        Map<AppendDirectory, Integer> directories = new HashMap<AppendDirectory, Integer>();

        List<AppendFile> dirFiles;
        List<AppendFile> files = new ArrayList<AppendFile>(mappedFiles.size());
        Integer index;
        Integer minUsedIndex;
        // 遍历缓存文件
        for (Map.Entry<AppendFile, MappedFile> entry : mappedFiles.entrySet()) {
            if (!isStarted()) {
                return;
            }
            // 当前文件
            file = entry.getKey();
            // 所属目录
            directory = file.directory();
            // 过滤掉待删除的文件
            if (releaseFiles.containsKey(file)) {
                continue;
            }

            if (!directories.containsKey(directory)) {
                // 获取目录文件下的索引
                dirFiles = directory.files();
                index = 0;
                minUsedIndex = null;
                // 遍历文件
                for (AppendFile af : dirFiles) {
                    // 计算最近使用了的最小文件位置
                    if (minUsedIndex == null && !isUnused(af)) {
                        minUsedIndex = index;
                    }
                    fileIndices.put(af, index++);
                }
                directories.put(directory, minUsedIndex);
            }
            // 获取文件索引，越小则创建时间越长
            index = fileIndices.get(file);
            minUsedIndex = directories.get(directory);

            if (index == null) {
                // 文件不存在了
                priorities.put(file, 100000000 - 1);
            } else if (isUnused(file)) {
                // 文件长时间没有使用了
                if (minUsedIndex != null && index < minUsedIndex) {
                    // 前面没有最近使用的文件
                    priorities.put(file, 100000000 + index);
                } else {
                    // 前面有最近使用的文件
                    priorities.put(file, 200000000 + index);
                }
            } else {
                //还在使用中
                priorities.put(file, 300000000 + index);
            }
            files.add(file);
        }

        // 按优先级从小到达排序
        Collections.sort(files, new Comparator<AppendFile>() {
            @Override
            public int compare(AppendFile o1, AppendFile o2) {
                return priorities.get(o1) - priorities.get(o2);
            }
        });

        // 按文件的优先级进行清理
        int priority;
        for (AppendFile af : files) {
            if (!isStarted()) {
                return;
            }
            priority = priorities.get(af);
            if (priority < 200000000 || forceCleanup > 0) {
                if (release(af, config.getCacheEvictLatency() > 0)) {
                    if (config.getCacheEvictLatency() > 0) {
                        // 延迟释放，增加待释放内存
                        releasingMemory += af.maxDataSize();
                    }
                    // 减少强制卸载内存
                    forceCleanup -= af.maxSize();
                    // 计算目前缓存占用
                    ratio = (int) ((fileStat.getMappedMemory() - releasingMemory) * 100 / config.getCacheSize());
                    // 小于新生代比率则直接退出
                    if (ratio <= config.getCacheNewRatio()) {
                        break;
                    }
                }
            } else {
                break;
            }
        }
    }

    /**
     * 释放等待队列中的缓存
     *
     * @return 待释放的缓存数据大小(不包括文件头)
     */
    protected long releaseCaches() {
        AppendFile file;// 计算待清理的内存大小
        long releasingMemory = 0;
        // 遍历待清理文件列表
        for (Map.Entry<AppendFile, Long> entry : releaseFiles.entrySet()) {
            if (!isStarted()) {
                return releasingMemory;
            }
            // 判断是否超过了待删除时间
            file = entry.getKey();
            if (SystemClock.now() - entry.getValue() > config.getCacheEvictLatency()) {
                if (!release(file, false)) {
                    releasingMemory += file.maxDataSize();
                }
            } else {
                releasingMemory += file.maxDataSize();
            }
        }
        return releasingMemory;
    }

    /**
     * 判断文件是否长时间未使用了
     *
     * @param file 文件
     * @return 长时间为使用标示
     */
    protected boolean isUnused(final AppendFile file) {
        return file.accessTime() > 0 && SystemClock.now() - file.accessTime() > config.getCacheMaxIdle();
    }

}

