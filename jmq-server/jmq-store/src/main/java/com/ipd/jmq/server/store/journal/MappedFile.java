package com.ipd.jmq.server.store.journal;

import com.ipd.jmq.toolkit.lang.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 内存镜像文件，目前映射整个文件。如果需要分段映射，可以考虑把原始文件分成多个小一点的文件
 */
public class MappedFile implements FileHandler, Closeable {
    // 通道文件
    protected ChannelFile file;
    // 文件统计
    protected FileStat fileStat;
    // 内存镜像缓冲区,用于追加写
    protected MappedByteBuffer mapped;
    // 数据位置
    protected int position;
    // 是否已经被清理
    protected AtomicBoolean started = new AtomicBoolean(false);

    public MappedFile(ChannelFile file, FileStat fileStat) throws IOException {
        Preconditions.checkArgument(file != null, "file can not be null");
        Preconditions.checkArgument(fileStat != null, "fileStat can not be null");
        this.file = file;
        this.fileStat = fileStat;
        this.position = file.position();
        try {
            // 整个文件映射
            mapped = file.getChannel().map(MapMode.READ_WRITE, 0, file.getSize());
            started.set(true);
            fileStat.mappedFiles.incrementAndGet();
            fileStat.mappedMemory.addAndGet(file.getMaxDataSize());
        } catch (IOException e) {
            throw JournalException.MappedFileException.build(e.getMessage(), e);
        }
    }

    @Override
    public int remaining() {
        return file.getMaxDataSize() - position;
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
        if (buffer == null) {
            return 0;
        } else if (!buffer.hasRemaining()) {
            return 0;
        } else if (remaining() < buffer.remaining()) {
            throw JournalException.InvalidPositionException.build(String
                    .format("exceed.size:%d,position:%d,data:%d", file.getMaxDataSize(), position, buffer.remaining()));
        }
        checkState();
        ByteBuffer slice = mapped.slice();
        slice.position(position + file.getHeadSize());
        if (buffer.hasArray()) {
            slice.put(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        } else {
            slice.put(buffer);
        }
        position += buffer.remaining();
        file.position(position);
        return buffer.remaining();
    }

    @Override
    public int write(final ByteBuffer buffer, final int position) throws IOException {
        if (buffer == null) {
            return 0;
        } else if (!buffer.hasRemaining()) {
            return 0;
        } else if (position < 0 || position >= file.getMaxDataSize()) {
            throw JournalException.InvalidPositionException.build(String
                    .format("position:%d is invalid. it must be in [0,%d).", position, file.getMaxDataSize()));
        } else if (file.getMaxDataSize() < position + buffer.remaining()) {
            throw JournalException.CapacityException.build(String
                    .format("exceed.size:%d,position:%d,data:%d", file.getMaxDataSize(), position, buffer.remaining()));
        }
        checkState();
        ByteBuffer slice = mapped.slice();
        slice.position(position + file.getHeadSize());
        if (buffer.hasArray()) {
            slice.put(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        } else {
            slice.put(buffer);
        }
        return buffer.remaining();
    }

    @Override
    public ByteBuffer read(final int position, final int size) throws IOException {
        if (size < 0) {
            throw new IOException("size must be greater than or equal 0");
        } else if (position < 0 || position >= file.getMaxDataSize()) {
            throw JournalException.InvalidPositionException.build(String
                    .format("position:%d is invalid. it must be in [0,%d).", position, file.getMaxDataSize()));
        }
        checkState();
        // 计算能读取的最大数据
        int minSize = Math.min(file.getMaxDataSize() - position, size);

        ByteBuffer slice = mapped.slice();
        slice.position(position + file.getHeadSize());
        slice.limit(position + file.getHeadSize() + minSize);

        return slice.slice();
    }

    @Override
    public void read(final ByteBuffer buffer, final int position, final int size) throws IOException {
        if (buffer == null) {
            throw new IOException("buffer can not be null");
        } else if (size < 0) {
            throw new IOException("size must be greater than or equal 0");
        } else if (position < 0 || position >= file.getMaxDataSize()) {
            throw JournalException.InvalidPositionException.build(String
                    .format("position:%d is invalid. it must be in [0,%d).", position, file.getMaxDataSize()));
        }
        checkState();
        // 计算能读取的最大数据
        int minSize = Math.min(buffer.capacity() - buffer.position(), Math.min(file.getMaxDataSize() - position, size));
        if (minSize == 0) {
            return;
        }

        ByteBuffer slice = mapped.slice();
        slice.position(position + file.getHeadSize());
        slice.limit(position + file.getHeadSize() + minSize);
        buffer.put(slice.slice());

    }

    @Override
    public int flush() throws IOException {
        checkState();
        int position = this.position;
        mapped.force();
        return position;
    }

    @Override
    public int position() {
        return position;
    }

    @Override
    public void position(final int position) throws IOException {
        // 允许position等于dataSize，因为position标示下一个写入位置
        if (position < 0 || position > file.getMaxDataSize()) {
            throw JournalException.InvalidPositionException.build(String
                    .format("position:%d is invalid. it must be in [0,%d].", position, file.getMaxDataSize()));
        }
        checkState();
        file.position(position);
        this.position = position;
    }

    @Override
    public void close() throws IOException {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        Exception error = AccessController.doPrivileged(new PrivilegedAction<Exception>() {
            public Exception run() {
                try {
                    Method getCleanerMethod = mapped.getClass().getMethod("cleaner", new Class[0]);
                    getCleanerMethod.setAccessible(true);
                    sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod.invoke(mapped, new Object[0]);
                    cleaner.clean();
                    return null;
                } catch (Exception e) {
                    return e;
                }
            }
        });
        fileStat.mappedFiles.decrementAndGet();
        fileStat.mappedMemory.addAndGet(-file.getMaxDataSize());
        if (error != null) {
            throw JournalException.MappedFileException
                    .build("release mmap file error." + file.getFile().getPath(), error);
        }
    }

    /**
     * 检查状态
     *
     * @throws IOException
     */
    protected void checkState() throws IOException {
        if (!started.get()) {
            throw JournalException.IllegalStateException.build("mapped file is closed." + file.file.getPath());
        }
    }
}