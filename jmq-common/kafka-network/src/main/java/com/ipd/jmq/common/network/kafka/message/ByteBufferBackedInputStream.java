package com.ipd.jmq.common.network.kafka.message;

import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created by zhangkepeng on 16-8-30.
 */
public class ByteBufferBackedInputStream extends InputStream {

    private ByteBuffer buffer;

    public ByteBufferBackedInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public int read() {
        if (buffer != null && buffer.hasRemaining()) {
            return (buffer.get() & 0xFF);
        } else {
            return -1;
        }
    }

    @Override
    public int read(byte[] bytes, int off, int len) {
        if (buffer != null && buffer.hasRemaining()) {
            int realLen = Math.min(len, buffer.remaining());
            buffer.get(bytes, off, realLen);
            return realLen;
        } else {
            return -1;
        }
    }
}
