package com.ipd.jmq.common.message;

import com.ipd.jmq.common.exception.ChecksumException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

/**
 * 消息队列中的的数据记录
 */
public class QueueItem {
    // 主题
    protected String topic;
    // 队列ID
    protected short queueId;
    // 队列偏移量
    protected long queueOffset;
    // 日志偏移量
    protected long journalOffset;
    // 大小
    protected int size;
    // 标签
    protected short flag;
    // 校验和
    protected long crc;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public short getQueueId() {
        return queueId;
    }

    public void setQueueId(short queueId) {
        this.queueId = queueId;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public long getJournalOffset() {
        return journalOffset;
    }

    public void setJournalOffset(long journalOffset) {
        this.journalOffset = journalOffset;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public short getFlag() {
        return flag;
    }

    public void setFlag(short flag) {
        this.flag = flag;
    }

    public long getCrc() {
        return crc;
    }

    public void setCrc(long crc) {
        this.crc = crc;
    }

    /**
     * 编码
     *
     * @param buf 缓冲区
     */
    public void encode(final ByteBuffer buf) {
        if (buf == null) {
            return;
        }
        ByteBuffer slice = buf.slice();
        int pos = buf.position();
        buf.putLong(journalOffset);
        buf.putInt(size);
        buf.putShort(flag);

        slice.limit(buf.position() - pos);

        buf.putLong(checksum(slice));
    }

    private long checksum(ByteBuffer crcBuf) {
        Checksum checksum = new Adler32();

        if (crcBuf.hasArray()) {
            checksum.update(crcBuf.array(), crcBuf.arrayOffset() + crcBuf.position(), crcBuf.remaining());
        } else {
            while (crcBuf.hasRemaining()) {
                checksum.update(crcBuf.get());
            }
        }
        return checksum.getValue();
    }

    /**
     * 解码
     *
     * @param buf 缓冲区
     */
    public void decode(final ByteBuffer buf) throws IOException {
        decode(buf, false);
    }

    /**
     * 解码
     *
     * @param buf   缓冲区
     * @param check 是否校验和
     * @throws ChecksumException
     */
    public void decode(final ByteBuffer buf, boolean check) throws ChecksumException {
        if (buf == null) {
            return;
        }
        ByteBuffer slice = buf.slice();
        int pos = buf.position();
        journalOffset = buf.getLong();
        size = buf.getInt();
        flag = buf.getShort();
        int limit = buf.position();
        crc = buf.getLong();

        if (size <= 0 || journalOffset < 0) {
            throw new ChecksumException(toString());
        }
        if (check) {
            slice.limit(limit - pos);
            long v = checksum(slice);
            if (v != crc) {
                throw new ChecksumException(String.format("queueItem checksum not equal v=%d,crc=%d", v, crc));
            }
        }

    }

    /**
     * 不请理topic 和 queueId
     */
    public void clear() {
        journalOffset = -1;
        size = 0;
        flag = 0;
        crc = 0;
        queueOffset=-1;

    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("QueueItem{");
        sb.append("topic='").append(topic).append('\'');
        sb.append(", queueId=").append(queueId);
        sb.append(", queueOffset=").append(queueOffset);
        sb.append(", journalOffset=").append(journalOffset);
        sb.append(", size=").append(size);
        sb.append(", flag=").append(flag);
        sb.append('}');
        return sb.toString();
    }
}
