package com.ipd.jmq.server.store.journal;

import com.ipd.jmq.toolkit.time.SystemClock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 文件头部实现，带上版本，创建时间和数据在文件中的偏移位置
 * Created by hexiaofeng on 16-7-13.
 */
public class VersionHeader extends EmptyHeader implements FileHeader {
    public final static short VERSION = 1;
    public final static short HEAD_SIZE = 2 + 8 + 8 + 46;//version+timestamp+empty
    // 版本
    protected short version = VERSION;

    public VersionHeader() {
    }

    public short getVersion() {
        return version;
    }

    public void setVersion(short version) {
        this.version = version;
    }

    @Override
    public int size() {
        return HEAD_SIZE;
    }

    @Override
    public String getFileName() {
        if (fileName == null) {
            if (file != null) {
                fileName = file.getName();
            } else {
                StringBuffer sb = new StringBuffer(100);
                sb.append(id);
                if (suffix != null && !suffix.isEmpty()) {
                    sb.append('.').append(suffix);
                }
                fileName = sb.toString();
            }
        }
        return fileName;
    }

    @Override
    public void update(final FileChannel channel) throws IOException {
        create(channel, false);
    }

    @Override
    public void create(final FileChannel channel) throws IOException {
        this.createTime = SystemClock.now();
        create(channel, true);
    }

    /**
     * 创建头部信息
     *
     * @param channel 通道
     * @param append  追加模式
     * @throws IOException
     */
    protected void create(final FileChannel channel, final boolean append) throws IOException {
        if (channel == null) {
            return;
        }
        int capacity = size();
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        // 版本
        buffer.putShort(version);
        // 时间戳
        buffer.putLong(createTime);
        // 有效数据偏移量
        buffer.putLong(position);
        // 预留空白数据
        for (int i = 0; i < (capacity - 2 - 8 - 8); i++) {
            buffer.put((byte) 0);
        }
        buffer.flip();
        try {
            if (append) {
                channel.write(buffer);
            } else {
                channel.write(buffer, 0);
            }
        } catch (IOException e) {
            throw JournalException.WriteFileException.build(e.getMessage(), e);
        }
    }

    @Override
    public void read(final FileChannel channel) throws IOException {
        int capacity = size();
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        int reads = 0;
        int len;
        int pos = 0;
        try {
            //  循环读取指定数据
            while (reads < capacity) {
                // 单次读取
                len = channel.read(buffer, pos);
                if (len >= 0) {
                    // 有数据
                    reads += len;
                    pos += len;
                } else {
                    // 达到文件末尾
                    break;
                }
            }
            buffer.flip();
            setVersion(buffer.getShort());
            setCreateTime(buffer.getLong());
            setPosition(buffer.getLong());
        } catch (IOException e) {
            throw JournalException.ReadFileException.build(e.getMessage(), e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        VersionHeader that = (VersionHeader) o;

        return version == that.version;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) version;
        return result;
    }

}
