package com.ipd.jmq.server.store.journal;


import com.ipd.jmq.toolkit.io.Files;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.service.Activity;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 追加目录
 */
public class AppendDirectory extends Activity {
    protected static Logger logger = LoggerFactory.getLogger(AppendDirectory.class);
    // 追加文件
    protected ConcurrentSkipListMap<Long, AppendFile> files = new ConcurrentSkipListMap<Long, AppendFile>();
    // 文件目录
    protected File directory;
    // 文件头
    protected FileHeader header;
    // 内存镜像文件管理器
    protected MappedFileManager cacheManager;
    // 数据最大长度，不包括头部文件
    protected JournalConfig config;
    // 文件统计
    protected FileStat fileStat;

    public AppendDirectory(File directory, JournalConfig config, FileHeader header, FileStat fileStat,
                           MappedFileManager cacheManager) {
        Preconditions.checkArgument(directory != null, "directory can not be null");
        Preconditions.checkArgument(config != null, "config can not be null");
        Preconditions.checkArgument(header != null, "header can not be null");

        this.directory = directory;
        this.config = config;
        this.header = header;
        this.fileStat = fileStat;
        this.cacheManager = cacheManager;
    }

    @Override
    protected void start() throws Exception {
        super.start();
    }

    @Override
    protected void validate() throws Exception {
        super.validate();
        if (!Files.createDirectory(directory)) {
            throw new IOException(String.format("create directory error. %s", directory));
        }
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        final List<AppendFile> fileList = new ArrayList<AppendFile>();
        directory.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                String name = file.getName();
                if (name.endsWith(config.getSuffix())) {
                    try {
                        // 头部文件要克隆一份
                        FileHeader clone = header.clone();
                        clone.reset();
                        clone.setFile(file);
                        clone.setSuffix(config.getSuffix());
                        AppendFile appendFile =
                                new AppendFile(file, config.getDataSize(), clone, fileStat, AppendDirectory.this,
                                        cacheManager);
                        fileList.add(appendFile);
                        return true;
                    } catch (Exception e) {
                        logger.error(String.format("invalid file name. %s", file.getPath()));
                    }
                }
                return false;
            }
        });
        if (!fileList.isEmpty()) {
            // 按文件名降序排序，便于缓存最新的文件
            Collections.sort(fileList, new Comparator<AppendFile>() {
                @Override
                public int compare(AppendFile o1, AppendFile o2) {
                    long id1 = o1.id();
                    long id2 = o2.id();

                    if (id1 > id2) {
                        return -1;
                    } else if (id1 < id2) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });

            // 遍历文件构造追加文件
            for (AppendFile file : fileList) {
                if (file.length() > 0 && file.length() != file.maxSize()) {
                    throw new IOException(
                            String.format("file length is invalid. expect %d,but %d. %s", file.maxSize(), file.length(),
                                    file.file().getPath()));
                }
                try {
                    file.start();
                    files.put(file.id(), file);
                } catch (IOException e) {
                    throw e;
                } catch (Exception e) {
                    throw JournalException.FatalException.build(e.getMessage(), e);
                }
            }

        }
        logger.info("open " + directory.getPath());
    }

    @Override
    protected void stop() {
        super.stop();
    }

    @Override
    protected void doStop() {
        // 遍历并关闭文件
        for (AppendFile file : files.values()) {
            file.stop();
        }
        files.clear();
        super.doStop();
        logger.info("close " + directory.getPath());
    }

    /**
     * 删除该目录下的所有文件
     */
    public void delete() {
        writeLock.lock();
        try {
            if (!isStarted()) {
                return;
            }
            if (!files.isEmpty()) {
                for (AppendFile file : files.values()) {
                    file.delete();
                }
                files.clear();
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 在启动恢复数据的时候，截取偏移量之后的数据，删除文件不需要加文件操作锁
     *
     * @param offset 全局数据偏移量(不包括文件头长度)
     * @throws IOException
     */
    public void truncate(final long offset) throws IOException {
        AppendFile file = null;
        long fileId = 0;
        writeLock.lock();
        try {
            checkState();
            if (files.isEmpty()) {
                return;
            }

            // 降序排序
            NavigableMap<Long, AppendFile> descs = files.descendingMap();
            for (Map.Entry<Long, AppendFile> entry : descs.entrySet()) {
                fileId = entry.getKey();
                file = entry.getValue();
                if (file.contain(offset)) {
                    // 包含改偏移量，则裁剪数据
                    int pos = (int) (offset % config.getDataSize());
                    logger.info(String.format("truncate to pos: %d,%s", pos, file.file().getPath()));
                    file.truncate(pos);
                    return;
                } else if (fileId > offset) {
                    // 前面已经判断了包含关系，则当前文件在偏移量之后，则删除
                    forceDelete(file);
                } else {
                    return;
                }
            }
        } catch (JournalException.IllegalStateException e) {
            throw e;
        } catch (IOException e) {
            if (file != null) {
                logger.error(String.format("truncate file error. %s", file.file().getPath()), e);
            } else {
                logger.error(String.format("truncate directory error. %s", directory.getPath()), e);
            }
            throw e;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 强制物理删除文件
     *
     * @param file 文件
     * @return 成功删除文件标示
     */
    protected boolean forceDelete(final AppendFile file) {
        if (file == null) {
            return true;
        }
        // 释放缓存
        if (cacheManager != null) {
            cacheManager.release(file, false);
        }
        // 删除文件
        file.delete();
        // 从目录中删除
        files.remove(file.id());
        logger.info(String.format("delete file success. %s", file.file().getPath()));
        return true;
    }

    /**
     * 安全物理删除文件
     *
     * @param file 文件
     * @return 成功删除文件标示
     */
    protected boolean safeDelete(final AppendFile file) {
        if (file == null) {
            return true;
        }
        boolean safe = true;
        if (cacheManager != null) {
            safe = cacheManager.release(file, false);
        }
        // 释放缓存
        if (safe) {
            // 删除文件
            file.delete();
            // 从目录中删除
            files.remove(file.id());
            logger.info(String.format("delete file success. %s", file.file().getPath()));
            return true;
        } else {
            logger.info(String.format("clean cache error, can not delete file. %s", file.file().getPath()));
            return false;
        }
    }

    /**
     * 删除指定位置之前的最老的一个文件
     *
     * @param offset        全局数据偏移量(不包括文件头长度)
     * @param retentionTime 文件保留时间(毫秒)
     * @return 是否成功
     * @throws IOException
     */
    public boolean deleteOldest(final long offset, final long retentionTime) throws IOException {
        writeLock.lock();
        try {
            checkState();
            if (files.isEmpty()) {
                return false;
            }
            if (retentionTime > 0 && files.size() < 2) {
                // 只剩下一个文件，需要保留
                return false;
            }
            // 第一个文件
            AppendFile file = files.firstEntry().getValue();
            // 判断文件数据是否在指定偏移量之前
            if (file.maxOffset() < offset) {
                boolean canDel = false;
                // 比较保留时间
                if (retentionTime <= 0) {
                    canDel = true;
                } else {
                    // 用下一个文件的创建时间来比较，确保当前文件已经保留超过了指定时间
                    AppendFile next = files.higherEntry(file.id()).getValue();
                    if (SystemClock.now() - next.header().getCreateTime() > retentionTime) {
                        canDel = true;
                    }
                }
                // 判断是否能删除
                if (canDel) {
                    // 加IO操作锁
                    file.getWriteLock().lock();
                    try {
                        return safeDelete(file);
                    } finally {
                        file.getWriteLock().unlock();
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }
        return false;
    }

    /**
     * 创建新文件，并追加到文件列表最后
     *
     * @return 文件
     * @throws IOException
     */
    public AppendFile create() throws IOException {
        writeLock.lock();
        try {
            checkState();
            return createIfNotExists(files.isEmpty() ? 0 : (files.lastKey() + config.getDataSize()), true);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 检查状态
     *
     * @throws IOException
     */
    protected void checkState() throws IOException {
        if (!isStarted()) {
            throw JournalException.IllegalStateException.build("directory is closed." + directory.getPath());
        }
    }

    /**
     * 创建文件
     *
     * @param id     名称
     * @param append 是否要追加到列表中
     * @return 文件
     * @throws IOException
     */
    protected AppendFile createIfNotExists(final long id, final boolean append) throws IOException {
        AppendFile result = files.get(id);
        if (result != null) {
            return result;
        }
        // 构造追加文件,头部要克隆一份
        FileHeader clone = this.header.clone();
        clone.reset();
        clone.setSuffix(config.getSuffix());
        clone.setId(id);
        clone.setCreateTime(SystemClock.now());

        File file = new File(directory, clone.getFileName());
        if (!Files.createFile(file)) {
            throw JournalException.CreateFileException.build(String.format("create file error. %s", file.getPath()));
        }
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("create file. %s", file.getPath()));
        }
        clone.setFile(file);
        result = new AppendFile(file, config.getDataSize(), clone, fileStat, this, cacheManager);
        try {
            // 启动文件
            result.start();
            // 放到缓存中
            if (append) {
                files.put(id, result);
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }

        return result;
    }

    /**
     * 获取第一个文件
     *
     * @return 第一个文件
     */
    public AppendFile first() {
        readLock.lock();
        try {
            return files.isEmpty() ? null : files.firstEntry().getValue();
        } finally {
            readLock.unlock();
        }

    }

    /**
     * 获取最后一个文件
     *
     * @return 最后一个文件
     */
    public AppendFile last() {
        readLock.lock();
        try {
            return files.isEmpty() ? null : files.lastEntry().getValue();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 获取最后一个文件，如果没用则创建一个
     *
     * @return 最后一个文件
     * @throws IOException
     */
    public AppendFile lastAndCreate() throws IOException {
        AppendFile file = last();
        if (file != null) {
            return file;
        }
        writeLock.lock();
        try {
            checkState();
            if (!files.isEmpty()) {
                return files.lastEntry().getValue();
            } else {
                return createIfNotExists(0, true);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 根据偏移量获取文件
     *
     * @param offset 全局数据偏移量(不包括文件头长度)
     * @return 文件
     */
    public AppendFile get(final long offset) {
        if (offset < 0) {
            return null;
        }
        readLock.lock();
        try {
            long fileId = offset - offset % config.getDataSize();
            return files.get(fileId);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 获取偏移量所在的文件，如果不存在则创建该文件
     *
     * @param offset 全局数据偏移量(不包括文件头长度)
     * @return 文件
     * @throws IOException
     */
    public AppendFile getAndCreate(final long offset) throws IOException {
        if (offset < 0) {
            throw JournalException.InvalidPositionException.build("offset must be greater than or equals 0");
        }
        AppendFile file = get(offset);
        if (file != null) {
            return file;
        }
        return createIfNotExists(offset - offset % config.getDataSize(), true);
    }

    /**
     * 根据全局偏移量获取文件，如果当前文件不存在，则创建从最后文件到该文件缺少的文件
     *
     * @param offset 全局数据偏移量(不包括文件头长度)
     * @return 创建的文件
     * @throws IOException
     */
    public AppendFile getAndCreatePrev(final long offset) throws IOException {
        return getAndCreatePrev(offset, false);
    }

    /**
     * 根据全局偏移量获取文件，如果当前文件不存在，则创建从最后文件到该文件缺少的文件
     *
     * @param offset  全局数据偏移量(不包括文件头长度)
     * @param recover 是否在应用启动恢复阶段
     * @return 文件
     * @throws IOException
     */
    public AppendFile getAndCreatePrev(final long offset, final boolean recover) throws IOException {
        if (offset < 0) {
            throw JournalException.InvalidPositionException.build("offset must be greater than or equals 0");
        }
        // 获取文件ID
        long fileId = offset - offset % config.getDataSize();
        // 文件中的相对位置
        int position = (int) (offset - fileId);
        AppendFile file;

        // 读锁
        readLock.lock();
        try {
            // 获取文件
            file = files.get(fileId);
            if (file != null) {
                // 恢复阶段
                if (recover) {
                    recoverHeader(file, position);
                }
                return file;
            }
        } finally {
            readLock.unlock();
        }

        // 加写锁
        writeLock.lock();
        try {
            // 二次获取文件进行判断
            file = files.get(fileId);
            if (file != null) {
                // 恢复阶段
                if (recover) {
                    recoverHeader(file, position);
                }
                return file;
            }

            // 判断是否是文件恢复阶段的第一个文件
            if (recover && files.size() == 1) {
                // 取到该文件
                file = files.lastEntry().getValue();
                // 如果文件ID为0，并且写入位置为0，并且偏移量在改文件只后
                if (file.id() == 0 && file.writePosition() == 0 && offset > file.maxOffset()) {
                    // 是初始化的一个无内容文件，可以删除
                    forceDelete(file);
                }
            }
            // 删除初始化的无内容文件后，判断是否还有文件
            if (!files.isEmpty()) {
                // 获取最后一个文件
                file = files.lastEntry().getValue();
            } else {
                // 创建第一个文件
                if (recover) {
                    file = createIfNotExists(fileId, false);
                    file.header().setPosition(position);
                    file.update(file.header());
                } else {
                    file = createIfNotExists(0, false);
                }
                files.put(file.id(), file);
            }

            // 从最后一个文件到当前偏移量创建缺失的文件
            long nextFileId = file.id() + config.getDataSize();
            while (nextFileId <= offset) {
                file = createIfNotExists(nextFileId, true);
                nextFileId = file.id() + config.getDataSize();
            }

            return files.get(fileId);
        } finally {
            writeLock.unlock();
        }

    }

    /**
     * 恢复一下头部信息
     *
     * @param file     文件
     * @param position 数据位置
     * @throws IOException
     */
    protected void recoverHeader(final AppendFile file, final int position) throws IOException {
        FileHeader header = file.header();
        // 头部记录的有效位置为0
        if (header.getPosition() == 0) {
            // 第一个文件，并且写入位置为0
            if (files.size() == 1 && file.writePosition() == 0 && position > file.writePosition()) {
                // 设置数据起始位置
                header.setPosition(position);
                file.update(header);
            }
        }
    }

    /**
     * 当前写入全局偏移量
     *
     * @return 当前写入全局偏移量，不包括文件头长度
     */
    public long writeOffset() {
        readLock.lock();
        try {
            AppendFile file = files.isEmpty() ? null : files.lastEntry().getValue();
            if (file == null) {
                return 0;
            }
            return file.id() + file.writePosition();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 获取当前刷盘的最大偏移量
     *
     * @return 当前刷盘全局偏移量，不包括文件头长度
     */
    public long flushOffset() {
        readLock.lock();
        try {
            AppendFile file = files.isEmpty() ? null : files.lastEntry().getValue();
            if (file == null) {
                return 0;
            }
            return file.id() + file.flushPosition();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 获取有效数据的最小偏移量
     *
     * @return 有效数据的最小偏移量
     */
    public long minOffset() {
        readLock.lock();
        try {
            AppendFile file = files.isEmpty() ? null : files.firstEntry().getValue();
            if (file == null) {
                return 0;
            }
            return file.id() + file.header.getPosition();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 根据全局偏移量得到文件中的偏移量
     *
     * @param offset 全局数据偏移量，不包括文件头长度
     * @return 文件中的偏移量
     */
    public long position(final long offset) {
        return offset % config.getDataSize();
    }

    /**
     * 获取追加文件列表
     *
     * @return 追加文件列表
     */
    public List<AppendFile> files() {
        readLock.lock();
        try {
            return new ArrayList<AppendFile>(files.values());
        } finally {
            readLock.unlock();
        }
    }

    public File directory() {
        return directory;
    }

}
