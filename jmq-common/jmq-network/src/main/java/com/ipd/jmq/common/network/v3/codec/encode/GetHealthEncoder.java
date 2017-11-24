package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.GetHealth;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 健康检测编码器
 */
public class GetHealthEncoder {

    public void encode(final GetHealth payload, final ByteBuf out) throws Exception {
        // 1字节应用长度
        Serializer.write(payload.getApp(), out);
        // 1字节主题
        Serializer.write(payload.getTopic(), out);
        // 1字节数据中心
        out.writeByte(payload.getDataCenter());
    }
}