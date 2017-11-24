package com.ipd.jmq.server.broker.log.serializer;

import com.ipd.jmq.toolkit.lang.Charsets;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * 序列化工具类
 */
public final class Serializer {

    /**
     * 写字符串
     *
     * @param buffer 字节缓冲区
     * @param text   字符串
     */
    public static void writeString(final ByteBuffer buffer, final String text) {
        writeString(buffer, text, null);
    }

    /**
     * 写字符串
     *
     * @param buffer  字节缓冲区
     * @param text    字符串
     * @param charset 字符集
     */
    public static void writeString(ByteBuffer buffer, String text, Charset charset) {
        if (text == null || text.isEmpty()) {
            buffer.putInt(0);
            return;
        }
        byte[] bytes = text.getBytes(charset == null ? Charsets.UTF_8 : charset);
        int length = bytes.length;
        buffer.putInt(length);
        buffer.put(bytes);
    }

    /**
     * 读取字符串
     *
     * @param buffer 字节缓冲区
     * @return 字符串
     */
    public static String readString(final ByteBuffer buffer) {
        return readString(buffer, null);
    }

    /**
     * 读取字符串
     *
     * @param buffer  字节缓冲区
     * @param charset 字符集
     * @return 字符串
     */
    public static String readString(final ByteBuffer buffer, final Charset charset) {
        if (buffer == null) {
            return null;
        }
        int length = buffer.getInt();
        if (length <= 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, charset == null ? Charsets.UTF_8 : charset);
    }

}
