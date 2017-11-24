package com.ipd.jmq.server.store.journal;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * 文件头信息
 * Created by hexiaofeng on 16-7-13.
 */
public interface FileHeader extends Cloneable {

    /**
     * 大小
     *
     * @return
     */
    int size();

    /**
     * 有效数据在文件内的起始位置
     *
     * @return 有效数据在文件内的起始位置(不包括文件头长度)
     */
    long getPosition();

    /**
     * 设置有效数据在文件内的起始位置
     *
     * @param position 有效数据在文件内的起始位置(不包括文件头长度)
     */
    void setPosition(long position);

    /**
     * 获取ID
     *
     * @return
     */
    long getId();

    /**
     * 设置文件序号
     *
     * @param id 序号
     */
    void setId(long id);

    /**
     * 设置文件名后缀
     *
     * @param suffix
     */
    void setSuffix(String suffix);

    /**
     * 获取文件名
     *
     * @return 文件名
     */
    String getFileName();

    /**
     * 设置文件
     *
     * @param file 当前文件
     */
    void setFile(File file);

    /**
     * 获取创建时间
     *
     * @return 创建时间
     */
    long getCreateTime();

    /**
     * 设置创建时间
     *
     * @param createTime 创建时间
     */
    void setCreateTime(long createTime);

    /**
     * 更新文件头部
     *
     * @param channel 文件通道
     * @throws IOException
     */
    void update(FileChannel channel) throws IOException;

    /**
     * 更新文件头部
     *
     * @param channel 文件通道
     * @throws IOException
     */
    void create(FileChannel channel) throws IOException;

    /**
     * 读取
     *
     * @param channel 文件通道
     * @throws IOException
     */
    void read(FileChannel channel) throws IOException;

    /**
     * 重置
     */
    void reset();

    /**
     * 克隆
     *
     * @return 克隆的数据
     */
    FileHeader clone();
}
