package com.ipd.jmq.server.store.journal;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 文件操作接口
 */
public interface FileHandler {

    /**
     * 追加数据
     *
     * @param data 字节数据
     * @return 写入的数据长度
     * @throws IOException
     */
    int append(byte[] data) throws IOException;

    /**
     * 追加缓冲区数据，写入数据异常则回滚到原有的写入位置
     *
     * @param buffer 缓冲区
     * @return 写入的数据长度
     * @throws IOException
     */
    int append(ByteBuffer buffer) throws IOException;

    /**
     * 在指定位置写入数据，写入数据异常不能回滚
     *
     * @param buffer   缓冲区
     * @param position 有效数据的写入位置，不包括固定的文件头长度
     * @return 写入的数据长度
     * @throws IOException
     */
    int write(ByteBuffer buffer, int position) throws IOException;

    /**
     * 刷盘，可以和写入并发，返回的位置为刷盘前的位置，可能小于实际刷盘的位置
     *
     * @return 安全刷盘的位置
     * @throws IOException
     */
    int flush() throws IOException;

    /**
     * 读取数据
     *
     * @param position 有效数据的读取位置，不包括固定的文件头长度
     * @param size     期望数据大小
     * @return 缓冲区
     * @throws IOException
     */
    ByteBuffer read(int position, int size) throws IOException;

    /**
     * 读取数据
     *
     * @param buffer   引用计数缓冲区
     * @param position 文件中的位置
     * @param size     期望数据大小
     * @throws IOException
     */
    void read(ByteBuffer buffer, int position, int size) throws IOException;

    /**
     * 剩余数据空间
     *
     * @return 剩余数据空间
     */
    int remaining();

    /**
     * 当前位置
     *
     * @return 当前位置
     */
    int position();

    /**
     * 设置当前位置
     *
     * @param position 有效数据的相对位置，不包括文件头的大小
     * @throws IOException
     */
    void position(int position) throws IOException;

}
