package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.network.command.Releasable;
import com.ipd.jmq.common.network.v3.codec.JMQCodec;
import com.ipd.jmq.common.network.v3.codec.encode.JMQEncoder;
import com.ipd.jmq.common.network.v3.netty.BufAllocator;
import com.ipd.jmq.common.network.netty.buffer.WrappedByteBuf;
import com.ipd.jmq.toolkit.buffer.RByteBuffer;
import com.ipd.jmq.toolkit.lang.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

/**
 * 复制，请求数据应答
 */
public class GetJournalAck extends JMQPayload implements BufAllocator, Releasable {
    // 偏移量
    private long offset;
    private boolean insync;
    // 数据
    private RByteBuffer buffer;

    private long checksum;

    public GetJournalAck offset(final long offset) {
        setOffset(offset);
        return this;
    }

    public GetJournalAck buffer(final RByteBuffer buffer) {
        setBuffer(buffer);
        return this;
    }

    public long getOffset() {
        return this.offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public boolean getInsync() {
        return insync;
    }

    public void setInsync(boolean insync) {
        this.insync = insync;
    }

    public RByteBuffer getBuffer() {
        return this.buffer;
    }

    public void setBuffer(RByteBuffer buffer) {
        this.buffer = buffer;
    }

    public long getChecksum() {
        return checksum;
    }

    public void setChecksum(long checksum) {
        this.checksum = checksum;
    }

    @Override
    public void release() {
        if (buffer != null) {
            buffer.release();
        }
    }

    protected int getSize(final int begin, final int end) {
        return super.getSize(begin, end) + getBufferSize();
    }

    public int getBufferSize() {
        return buffer == null ? 0 : buffer.remaining();
    }

    public int predictionSize() {
        return 8 + 8 + 4 + 1;
    }


    @Override
    public void validate() {
        super.validate();
        Preconditions.checkArgument(offset > 0, "offset must be greater than or equal 0");
    }

    @Override
    public int type() {
        return CmdTypes.GET_JOURNAL_ACK;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GetJournalAck{");
        sb.append("offset=").append(offset);
        sb.append(",inSync=").append(insync);
        if (buffer != null) {
            sb.append(", size=").append(buffer.remaining());
        }
        sb.append('}');
        return sb.toString();
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

        GetJournalAck that = (GetJournalAck) o;

        if (offset != that.offset) {
            return false;
        }
        if (insync != that.insync) {
            return false;
        }
        if (buffer != null ? !buffer.equals(that.buffer) : that.buffer != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        result = 31 * result + (buffer != null ? buffer.hashCode() : 0);
        return result;
    }


    @Override
    public ByteBuf allocate(ChannelHandlerContext ctx, boolean preferDirect, Object attach) throws Exception {
        ByteBuf byteBuf;
        if (preferDirect) {
            byteBuf = ctx.alloc().ioBuffer();
        } else {
            byteBuf = ctx.alloc().heapBuffer();
        }

        // 序列化头部
        //写magic
        byteBuf.writeShort(JMQCodec.MAGIC);
        //记录起始位置
        int begin = byteBuf.writerIndex();

        int capacity = begin + ((JMQHeader) attach).getPredictionSize() + this.predictionSize();
        if (byteBuf.capacity() < capacity) {
            byteBuf.capacity(capacity);
        }

        byteBuf.writeInt(0);

        encodeAttach(byteBuf, attach);

        // 序列化payload部分
        encodeBody(byteBuf);

        int end = byteBuf.writerIndex();
        byteBuf.writerIndex(begin);
        byteBuf.writeInt(getSize(begin, end));
        byteBuf.writerIndex(end);

        // 输出头部数据
        ctx.write(byteBuf, ctx.voidPromise());

        // 构造数据块缓冲区
        if (buffer == null) {
            return Unpooled.EMPTY_BUFFER;
        }
        return new WrappedByteBuf(buffer);
    }


    private void encodeAttach(ByteBuf out, Object attach) throws Exception {
        JMQEncoder encoder = new JMQEncoder();
        encoder.encode((JMQHeader) attach, out);
    }

    private void encodeBody(final ByteBuf out) throws Exception {
        // 8字节偏移量
        out.writeLong(offset);
        out.writeBoolean(insync);
        out.writeLong(checksum);
        out.writeInt(getBufferSize());
    }

}