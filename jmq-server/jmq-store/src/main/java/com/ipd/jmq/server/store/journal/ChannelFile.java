package com.ipd.jmq.server.store.journal;

import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.lang.Preconditions;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 通道文件，非线程安全
 */
public class ChannelFile implements FileHandler, Closeable {
    // 文件
    protected File file;
    // 文件通道
    protected FileChannel channel;
    // 有效数据最大长度，不包括文件头大小
    protected int maxDataSize;
    // 有效数据的下一个写入位置
    protected int position;
    // 文件头部大小
    protected int headSize;

    public ChannelFile(File file, FileChannel channel, int maxDataSize) {
        this(file, channel, 0, maxDataSize, 0);
    }

    public ChannelFile(File file, FileChannel channel, int position, int maxDataSize) {
        this(file, channel, position, maxDataSize, 0);
    }

    public ChannelFile(File file, FileChannel channel, int position, int maxDataSize, int headSize) {
        Preconditions.checkArgument(file != null, "file can not be null");
        Preconditions.checkArgument(channel != null, "channel can not be null");
        Preconditions.checkArgument(maxDataSize > 0, "maxDataSize must be greater than 0");
        Preconditions.checkArgument(position >= 0, "position must be greater than or equals 0");
        this.file = file;
        this.channel = channel;
        this.maxDataSize = maxDataSize;
        this.headSize = headSize <= 0 ? 0 : headSize;
        this.position = position;
    }

    public File getFile() {
        return file;
    }

    @Override
    public int remaining() {
        return maxDataSize - position;
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
            throw JournalException.CapacityException.build(String
                    .format("exceed.size:%d,position:%d,data:%d", maxDataSize, position, buffer.remaining()));
        }
        checkState();
        // 不能确保一次写入多少数据，需要循环
        int len = 0;
        try {
            while (buffer.hasRemaining()) {
                len += channel.write(buffer);
            }
            position += len;
            return len;
        } catch (IOException e) {
            // 写入了数据
            if (channel.position() > position + headSize) {
                // 逻辑删除，定位到原来的位置
                try {
                    position(position);
                } catch (IOException ex) {
                    throw JournalException.WriteFileException.build(e.getMessage(), e);
                }
            }
            throw JournalException.WriteFileException.build(e.getMessage(), e);
        }
    }

    @Override
    public int write(final ByteBuffer buffer, final int position) throws IOException {
        if (buffer == null) {
            return 0;
        } else if (!buffer.hasRemaining()) {
            return 0;
        } else if (position < 0 || position >= maxDataSize) {
            throw JournalException.InvalidPositionException
                    .build(String.format("position:%d is invalid. it must be in [0,%d).", position, maxDataSize));
        } else if (maxDataSize < position + buffer.remaining()) {
            throw JournalException.CapacityException.build(String
                    .format("exceed.size:%d,position:%d,data:%d", maxDataSize, position, buffer.remaining()));
        }
        checkState();
        // 不能确保一次写入多少数据，需要循环
        try {
            int offset = position;
            while (buffer.hasRemaining()) {
                offset += channel.write(buffer, offset + headSize);
            }
            return offset - position;
        } catch (IOException e) {
            throw JournalException.WriteFileException.build(e.getMessage(), e);
        }
    }

    @Override
    public int flush() throws IOException {
        checkState();
        try {
            int position = this.position;
            channel.force(true);
            return position;
        } catch (IOException e) {
            throw JournalException.WriteFileException.build(e.getMessage(), e);
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
        checkState();

        // 计算能读取的最大数据
        int minSize = Math.min(maxDataSize - position, size);
        ByteBuffer buf = ByteBuffer.allocate(minSize);
        if (minSize == 0) {
            buf.flip();
            return buf;
        }
        read(buf, position, minSize);
        buf.flip();
        return buf;
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
        }
        checkState();

        // 计算能读取的最大数据
        int minSize = Math.min(buffer.capacity() - buffer.position(), Math.min(maxDataSize - position, size));
        if (minSize == 0) {
            return;
        }
        read0(buffer, position, minSize);
    }

    /**
     * 尝试读取数据
     *
     * @param buffer   缓冲区
     * @param position 有效数据的位置，不包括文件头大小
     * @param size     期望大小
     * @return 实际读取的数据
     * @throws IOException
     */
    protected int read0(final ByteBuffer buffer, final int position, final int size) throws IOException {
        int reads = 0;
        int len;
        int pos = position;
        //  循环读取指定数据
        try {
            while (reads < size) {
                // 单次读取
                len = channel.read(buffer, pos + headSize);
                if (len >= 0) {
                    // 有数据
                    reads += len;
                    pos += len;
                } else {
                    // 达到文件末尾
                    break;
                }
            }
            return reads;
        } catch (IOException e) {
            throw JournalException.ReadFileException.build(e.getMessage(), e);
        }
    }

    @Override
    public int position() {
        return position;
    }

    @Override
    public void position(final int position) throws IOException {
        // 允许position等于dataSize，因为position标示下一个写入位置
        if (position < 0 || position > maxDataSize) {
            throw JournalException.InvalidPositionException
                    .build(String.format("position:%d is invalid. it must be in [0,%d].", position, maxDataSize));
        }
        checkState();
        try {
            channel.position(position + headSize);
            this.position = position;
        } catch (IOException e) {
            throw JournalException.PositionException.build(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        Close.close(channel);
    }

    /**
     * 检查状态
     *
     * @throws IOException
     */
    protected void checkState() throws IOException {
        if (!channel.isOpen()) {
            throw JournalException.IllegalStateException.build("channel is closed." + file.getPath());
        }
    }

    public FileChannel getChannel() {
        return channel;
    }

    public int getMaxDataSize() {
        return maxDataSize;
    }

    public int getHeadSize() {
        return headSize;
    }

    public int getSize() {
        return maxDataSize + headSize;
    }

}