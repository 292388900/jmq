package com.ipd.jmq.server.store.journal;

import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.ref.Reference;
import com.ipd.jmq.toolkit.ref.ReferenceCounter;
import com.ipd.jmq.toolkit.service.Activity;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;

/**
 * 追加文件，单线程写入，多线程读。读写操作和删除清理操作互斥。<br/>
 * 文件的类型通过后缀来判断
 */
public class AppendFile extends Activity implements Reference, FileHandler {
    protected static final Logger logger = LoggerFactory.getLogger(AppendFile.class);
    // 文件
    protected File file;
    // 所属目录
    protected AppendDirectory directory;
    // 内存镜像文件管理器(用于缓存本身文件)
    protected MappedFileManager mappedFileManager;
    // 文件统计
    protected FileStat fileStat;
    // 最大文件数据长度，不包含文件头的长度
    protected int maxDataSize;
    // 写入的位置,不包含文件头的长度
    protected volatile int writePosition;
    // 刷入磁盘的位置,不包含文件头的长度
    protected volatile int flushPosition;
    // 文件IO操作(切换MappedFile和ChannelFile)
    protected volatile FileHandler handler;
    // 文件序号
    protected long id = 0;
    // 文件通道
    protected FileChannel fileChannel;
    // 随机访问文件
    protected RandomAccessFile raf;
    // 有效数据在文件中的开始位置
    protected FileHeader header;
    // 文件通道操作接口实现
    protected ChannelFile channelFile;
    // 头部文件大小
    protected int headSize;
    // 计数器
    protected Reference reference = new ReferenceCounter();
    // 写锁
    protected Object writeMutex = new Object();
    // 刷盘锁
    protected Object flushMutex = new Object();
    // 使用时间
    protected long accessTime;

    public AppendFile(File file, int maxDataSize, FileHeader header, FileStat fileStat) {
        this(file, maxDataSize, header, fileStat, null, null);
    }

    public AppendFile(File file, int maxDataSize, FileHeader header, FileStat fileStat,
                      MappedFileManager mappedFileManager) {
        this(file, maxDataSize, header, fileStat, null, mappedFileManager);
    }

    public AppendFile(File file, int maxDataSize, FileHeader header, FileStat fileStat, AppendDirectory directory,
                      MappedFileManager mappedFileManager) {
        Preconditions.checkArgument(file != null, "file can not be null");
        Preconditions.checkArgument(maxDataSize > 0, "fileLength must be greater than 0");
        Preconditions.checkArgument(header != null, "header can not be null");
        Preconditions.checkArgument(fileStat != null, "fileStat can not be null");
        Preconditions.checkArgument(directory != null, "directory can not be null");

        this.id = header.getId();
        this.file = file;
        this.maxDataSize = maxDataSize;
        this.header = header;
        this.headSize = header.size();
        this.directory = directory;
        this.mappedFileManager = mappedFileManager;
        this.fileStat = fileStat;
    }

    @Override
    protected void start() throws Exception {
        super.start();
    }

    @Override
    protected void doStart() throws IOException {
        raf = new RandomAccessFile(file, "rw");
        fileChannel = raf.getChannel();
        channelFile = new ChannelFile(file, fileChannel, 0, maxDataSize, headSize);
        // 文件长度
        long length = file.length();
        int maxLength = maxSize();
        // 判断头部数据是否存在
        if (length <= headSize) {
            // 创建头部数据
            flushPosition = 0;
            writePosition = 0;
            header.create(fileChannel);
            // 修改文件长度
            changeLength(raf, maxLength);
        } else {
            // 读取头部数据
            header.read(fileChannel);
            // 判断文件长度
            if (length != maxLength) {
                // 重置文件长度
                if (length > maxLength) {
                    logger.warn(
                            String.format("file:%s length(%d) will truncate to %d", file.getPath(), length, maxLength));
                }
                changeLength(raf, maxLength);
            }
            // 设置到文件末尾，在恢复阶段重置写入位置和刷盘位置
            flushPosition = maxDataSize;
            writePosition = maxDataSize;
        }
        // 缓存
        if (mappedFileManager != null) {
            // 目前内存出了问题也抛出异常，避免速度太慢和内存不可靠
            handler = mappedFileManager.cache(this);
        }
        if (handler == null) {
            handler = channelFile;
        }
        handler.position(writePosition);
        // 文件统计
        fileStat.appendFiles.incrementAndGet();
        fileStat.appendFileLength.addAndGet(writePosition);
    }

    @Override
    protected void stop() {
        super.stop();
    }

    @Override
    protected void doStop() {
        if (handler != null) {
            // 有数据需要刷盘
            if (flushPosition < writePosition) {
                try {
                    // 刷盘
                    handler.flush();
                    flushPosition = writePosition;
                } catch (IOException ignored) {
                }
            }
            // 关闭
            if (handler instanceof Closeable) {
                Close.close((Closeable) handler);
            }
        }
        Close.close(channelFile).close(fileChannel).close(raf);
        fileChannel = null;
        raf = null;
        handler = null;
        channelFile = null;
        fileStat.appendFiles.decrementAndGet();
        fileStat.appendFileLength.addAndGet(-writePosition);
        if (logger.isDebugEnabled()) {
            logger.debug("close file " + file.getPath());
        }
    }

    @Override
    protected boolean isStarted() {
        return started.get();
    }

    @Override
    protected ServiceState getServiceState() {
        return super.getServiceState();
    }

    @Override
    protected Lock getReadLock() {
        return readLock;
    }

    @Override
    protected Lock getWriteLock() {
        return writeLock;
    }

    /**
     * 停止并删除文件，不要暴露给外面
     */
    protected void delete() {
        stop(new Runnable() {
            @Override
            public void run() {
                if (file.exists()) {
                    file.delete();
                    logger.info("delete file:" + file.getPath());
                }
            }
        });
    }

    @Override
    public int append(final byte[] data) throws IOException {
        if (data == null || data.length == 0) {
            return 0;
        }
        return append(ByteBuffer.wrap(data, 0, data.length));
    }

    @Override
    public int append(final ByteBuffer buffer) throws IOException {
        if (buffer == null || !buffer.hasRemaining()) {
            return 0;
        }
        // 追加模式，在写的时候可以并发读取
        readLock.lock();
        try {
            checkState();
            // 顺序写
            synchronized (writeMutex) {
                if (writePosition != handler.position()) {
                    throw JournalException.WriteFileException
                            .build(String.format("append position error.%d!=%d", writePosition, handler.position()));
                }
                int len = handler.append(buffer);
                writePosition += len;
                fileStat.appendFileLength.addAndGet(len);
                return len;
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 数据快速拷贝，不会修改当前文件通道的位置
     *
     * @param target   目标追加文件
     * @param position 有效数据位置，不包括文件头部
     * @param length   长度
     * @return 实际拷贝的数据
     * @throws IOException
     */
    protected long transferTo(final AppendFile target, final long position, final int length) throws IOException {
        readLock.lock();
        try {
            checkState();
            long size = 0;
            while (size < length) {
                // 不修改当前文件通道的位置
                size += fileChannel.transferTo(position + size + headSize, length - size, target.fileChannel);
            }
            return size;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int write(final ByteBuffer buffer, final int position) throws IOException {
        if (buffer == null || !buffer.hasRemaining()) {
            return 0;
        } else if (position < 0 || position >= maxDataSize) {
            throw JournalException.InvalidPositionException
                    .build(String.format("position:%d is invalid. it must be in [0,%d).", position, maxDataSize));
        } else if (maxDataSize < position + buffer.remaining()) {
            throw JournalException.CapacityException.build(String
                    .format("exceed.size:%d,position:%d,data:%d", maxDataSize, position, buffer.remaining()));
        }
        // 防止在删除或停止
        readLock.lock();
        try {
            checkState();
            int len;
            boolean flag = false;
            // 单线程
            synchronized (writeMutex) {
                len = handler.write(buffer, position);
                int pos = position + len;
                int appends = pos - writePosition;
                // 超过了原来的写入位置
                if (appends > 0) {
                    // 重新定位写入位置
                    handler.position(pos);
                    writePosition = pos;
                    fileStat.appendFileLength.addAndGet(appends);
                    flag = true;
                }
            }
            // 刷盘位置
            if (flag) {
                synchronized (flushMutex) {
                    if (flushPosition > position) {
                        flushPosition = position;
                    }
                }
            }
            return len;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 复制文件，不修改源文件的读写位置。<br/>
     * 数据写完后，当前文件写入位置定位到最新写入数据后。
     *
     * @param src     源文件
     * @param srcPos  源文件起始位置
     * @param destPos 目标文件位置
     * @param length  最大长度
     * @return 实际写入的数量
     * @throws IOException
     */
    public long write(final AppendFile src, final int srcPos, final int destPos, final int length) throws IOException {
        if (src == null || length <= 0) {
            return 0;
        }
        FileChannel srcChannel = src.fileChannel;
        int srcHeadSize = src.headSize();
        long srcSize = srcChannel.size() - srcHeadSize;
        if (srcPos < 0 || srcPos >= srcSize) {
            throw JournalException.InvalidPositionException.build("srcPos must be in [0," + srcSize + ")");
        }
        // 计算能读取的数据
        int capacity = Math.min((int) (srcSize - srcPos), length);
        if (capacity <= 0) {
            return 0;
        }
        // 判断是否能写入这么多数据
        if (maxDataSize - destPos < capacity) {
            throw JournalException.CapacityException
                    .build(String.format("exceed.size:%d,position:%d,data:%d", maxDataSize, destPos, capacity));
        }
        // 防止在删除或停止
        readLock.lock();
        try {
            checkState();
            long size = 0;
            // 写锁
            synchronized (writeMutex) {
                try {
                    if (writePosition != destPos) {
                        // 把当前通道定位到写入位置，在写锁里面，线程安全
                        fileChannel.position(destPos + headSize);
                    }
                    // 拷贝数据
                    size = src.transferTo(this, srcPos, capacity);
                    if (size > 0) {
                        // 重新设置写入位置
                        int position = (int) (destPos + size);
                        int appends = position - writePosition;
                        handler.position(position);
                        writePosition = position;
                        fileStat.appendFileLength.addAndGet(appends);
                    }
                } catch (JournalException e) {
                    throw e;
                } catch (IOException e) {
                    throw JournalException.WriteFileException.build(e.getMessage(), e);
                }
            }
            if (size > 0) {
                // 备份一下
                long lastFlush = flushPosition;
                // 更新刷盘位置
                synchronized (flushMutex) {
                    // 防止已经刷盘了
                    if (flushPosition == lastFlush && flushPosition > destPos) {
                        flushPosition = destPos;
                    }
                }
            }
            return size;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int flush() throws IOException {
        // 防止在删除或停止
        readLock.lock();
        try {
            checkState();
            // 刷盘和写入可以并发操作，不需要写锁
            synchronized (flushMutex) {
                // 当前写入位置
                int position = writePosition;
                if (flushPosition < position) {
                    // 有数据需要刷盘
                    handler.flush();
                    // 实际的刷盘位置可能大约备份的位置
                    flushPosition = position;
                } else if (flushPosition > position) {
                    // 数据进行了截断操作，重置刷盘位置
                    flushPosition = position;
                }
                return flushPosition;
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 卸载内存镜像后调用，切换到通道文件操作。<br/>
     * 如果切换失败，致命错误则禁止读写操作。
     *
     * @throws IOException
     */
    protected void unmapped() throws IOException {
        if (!isStarted()) {
            return;
        }
        // 拿到写锁（这个时候不能读取，不能写入和刷盘），强制清理
        writeLock.lock();
        try {
            if (!isStarted() || handler == null || handler == channelFile) {
                // 关闭了；上次切换失败；本身已经是通道文件
                return;
            }
            // 该方法由内存镜像管理的清理线程调用
            FileHandler target = handler;
            handler = null;
            if (flushPosition < writePosition) {
                // 再次刷盘，避免数据没有写到磁盘里面，为了性能，最好前面也进行刷盘一次
                target.flush();
                flushPosition = writePosition;
            }
            channelFile.position(writePosition);
            handler = channelFile;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 裁剪数据，只移动指针，不实际清除数据。可以并发读，不能并发写和刷盘。
     *
     * @param position 位置
     * @throws IOException
     */
    protected void truncate(final int position) throws IOException {
        if (position < 0 || position >= maxDataSize) {
            throw JournalException.InvalidPositionException.build("position must be in [0," + maxDataSize + ")");
        }
        readLock.lock();
        try {
            checkState();

            // 写入位置，这个时候可能在并发刷盘
            synchronized (writeMutex) {
                if (writePosition > position) {
                    int appends = position - writePosition;
                    handler.position(position);
                    writePosition = position;
                    fileStat.appendFileLength.addAndGet(appends);
                }
            }
            // 备份上次刷盘位置
            int lastFlush = flushPosition;
            synchronized (flushMutex) {
                if (lastFlush == flushPosition) {
                    // 没有并发刷盘
                    if (flushPosition > position) {
                        flushPosition = position;
                    }
                } else if (flushPosition > writePosition) {
                    flushPosition = writePosition;
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 更新文件头部
     *
     * @param header 文件头部
     * @throws IOException
     */
    public void update(final FileHeader header) throws IOException {
        if (header == null) {
            return;
        }
        // 防止在删除或停止
        writeLock.lock();
        try {
            checkState();
            this.header = header;
            header.update(fileChannel);
        } catch (IOException e) {
            throw e;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public ByteBuffer read(final int position, final int size) throws IOException {
        if (size < 0) {
            throw new IOException("size must be greater than or equal 0");
        } else if (position < 0 || position >= maxDataSize) {
            throw JournalException.InvalidPositionException
                    .build(String.format("position:%d is invalid. it must be in [0,%d).", position, maxDataSize));
        }
        // 防止在删除或停止
        readLock.lock();
        try {
            checkState();
            accessTime = SystemClock.now();
            // TODO 尝试包装增加引用计数器
            return handler.read(position, Math.min(writePosition - position, size));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void read(final ByteBuffer buffer, final int position, final int size) throws IOException {
        if (buffer == null) {
            throw new IOException("buffer can not be null");
        } else if (size < 0) {
            throw new IOException("size must be greater than or equal 0");
        } else if (position < 0 || position >= maxDataSize) {
            throw JournalException.InvalidPositionException
                    .build(String.format("position:%d is invalid. it must be in [0,%d).", position, maxDataSize));
        } else if (size == 0) {
            return;
        }
        // 防止在删除或停止
        readLock.lock();
        try {
            checkState();
            accessTime = SystemClock.now();
            handler.read(buffer, position, Math.min(writePosition - position, size));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int remaining() {
        return maxDataSize - writePosition;
    }

    @Override
    public int position() {
        return writePosition;
    }

    @Override
    public void position(final int position) throws IOException {
        readLock.lock();
        try {
            // 写位置
            synchronized (writeMutex) {
                int appends = position - writePosition;
                handler.position(position);
                writePosition = position;
                fileStat.appendFileLength.addAndGet(appends);
            }
            // 刷盘位置
            synchronized (flushMutex) {
                if (flushPosition > writePosition) {
                    flushPosition = writePosition;
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void acquire() {
        reference.acquire();
    }

    @Override
    public long references() {
        return reference.references();
    }

    @Override
    public boolean release() {
        return reference.release();
    }

    /**
     * 修改文件长度
     *
     * @param raf    文件
     * @param length 长度
     * @throws IOException
     */
    protected void changeLength(final RandomAccessFile raf, final int length) throws IOException {
        try {
            raf.setLength(length);
        } catch (IOException e) {
            throw JournalException.CreateFileException.build(e.getMessage(), e);
        }
    }

    /**
     * 检查状态
     *
     * @throws IOException
     */
    protected void checkState() throws IOException {
        if (!isStarted()) {
            throw JournalException.IllegalStateException.build("append file is closed." + file.getPath());
        }
        if (handler == null) {
            // 卸载内存镜像出错，致命错误
            throw JournalException.MappedFileException.build("unmapped file error." + file.getPath());
        }
    }

    public File file() {
        return file;
    }

    public AppendDirectory directory() {
        return directory;
    }

    public ChannelFile channelFile() {
        return channelFile;
    }

    public int writePosition() {
        return writePosition;
    }

    public int flushPosition() {
        return flushPosition;
    }

    public int maxDataSize() {
        return maxDataSize;
    }

    /**
     * 获取头部数据大小
     *
     * @return 头部数据大小
     */
    public int headSize() {
        return headSize;
    }

    /**
     * 文件实际大小
     *
     * @return 实际大小
     */
    public long length() {
        return file.length();
    }

    /**
     * 最大长度(数据最大长度+头部长度)
     *
     * @return
     */
    public int maxSize() {
        return maxDataSize + headSize;
    }

    /**
     * 最大偏移位置(不包括文件头长度)
     *
     * @return 最大偏移位置
     */
    public long maxOffset() {
        return id + maxDataSize - 1;
    }

    public long id() {
        return id;
    }

    /**
     * 是否包含该偏移量
     *
     * @param offset 偏移量
     * @return 该偏移量是否在文件范围之内
     */
    public boolean contain(final long offset) {
        return offset >= id && offset <= maxOffset();
    }


    /**
     * 获取最近访问时间
     *
     * @return 最近访问时间
     */
    public long accessTime() {
        return accessTime;
    }

    /**
     * 获取文件头部
     *
     * @return
     */
    public FileHeader header() {
        return header;
    }

}