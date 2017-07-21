package com.ipd.jmq.server.broker.log.serializer;



import com.ipd.jmq.toolkit.plugin.ServicePlugin;
import com.ipd.jmq.toolkit.plugin.Share;

import java.nio.ByteBuffer;

/**
 * @author hexiaofeng
 */
@Share
public interface LogSerializer extends ServicePlugin {
    /**
     * 序列化对象
     *
     * @param item   对象
     * @param buffer 内存缓冲区
     * @return 缓冲区
     * @throws Exception
     */
    public ByteBuffer encode(Object item, ByteBuffer buffer) throws Exception;

    /**
     * 反序列化数据，buffer应该跳过了长度
     *
     * @param buffer 内存缓冲区
     * @param offset 文件中的位置
     * @return item
     * @throws Exception
     */
    public Object decode(ByteBuffer buffer, long offset) throws Exception;
}