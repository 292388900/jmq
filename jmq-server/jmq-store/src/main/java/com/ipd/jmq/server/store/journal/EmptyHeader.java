package com.ipd.jmq.server.store.journal;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * 空文件头部实现
 * Created by hexiaofeng on 16-7-13.
 */
public class EmptyHeader implements FileHeader, Cloneable {
    // 时间戳
    protected long createTime = 0;
    // 有效数据在文件内的起始位置(不包括文件头长度)
    protected long position = 0;
    // 文件序号
    protected long id = -1;
    // 后缀名称
    protected String suffix;
    // 文件名称
    protected String fileName;
    // 文件
    protected File file;

    public EmptyHeader() {
    }

    public EmptyHeader(long createTime) {
        this.createTime = createTime;
    }

    @Override
    public long getCreateTime() {
        return createTime;
    }

    @Override
    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public long getPosition() {
        return position;
    }

    @Override
    public void setPosition(long position) {
        this.position = position;
    }

    @Override
    public long getId() {
        if (id < 0) {
            String name = getFileName();
            if (name != null) {
                String[] parts = name.split("\\.");
                id = Long.parseLong(parts[0]);
            }
        }
        return id;
    }

    @Override
    public void setId(long id) {
        this.id = id;
    }

    @Override
    public void setSuffix(String suffix) {
        this.suffix = suffix;
    }

    @Override
    public String getFileName() {
        if (fileName == null) {
            if (file != null) {
                fileName = file.getName();
            } else {
                StringBuffer sb = new StringBuffer(100);
                sb.append(id).append('.').append(createTime);
                if (suffix != null && !suffix.isEmpty()) {
                    sb.append('.').append(suffix);
                }
                fileName = sb.toString();
            }
        }
        return fileName;
    }

    @Override
    public void setFile(File file) {
        this.file = file;
    }

    @Override
    public void read(final FileChannel channel) throws IOException {
        String name = getFileName();
        String[] parts = name.split("\\.");
        if (suffix != null && !suffix.isEmpty()) {
            if (parts.length >= 3) {
                createTime = Long.parseLong(parts[1]);
            }
        } else if (parts.length >= 2) {
            createTime = Long.parseLong(parts[1]);
        }
    }

    @Override
    public void update(final FileChannel channel) throws IOException {
    }

    @Override
    public void create(final FileChannel channel) throws IOException {
    }

    @Override
    public void reset() {
        createTime = 0;
        position = 0;
        id = -1;
        fileName = null;
        file = null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EmptyHeader that = (EmptyHeader) o;

        if (createTime != that.createTime) {
            return false;
        }
        if (position != that.position) {
            return false;
        }
        if (id != that.id) {
            return false;
        }
        return suffix != null ? suffix.equals(that.suffix) : that.suffix == null;

    }

    @Override
    public int hashCode() {
        int result = (int) (createTime ^ (createTime >>> 32));
        result = 31 * result + (int) (position ^ (position >>> 32));
        result = 31 * result + (int) (id ^ (id >>> 32));
        result = 31 * result + (suffix != null ? suffix.hashCode() : 0);
        return result;
    }

    @Override
    public FileHeader clone() {
        try {
            return (FileHeader) super.clone();
        } catch (CloneNotSupportedException ignored) {
        }
        return null;
    }
}
