package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.network.v3.codec.JMQCodec;
import com.ipd.jmq.common.network.v3.codec.encode.JMQEncoder;
import com.ipd.jmq.common.network.v3.netty.BufAllocator;
import com.ipd.jmq.common.network.v3.netty.buffer.WrappedByteBuf;
import com.ipd.jmq.toolkit.buffer.RByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * 请求消息应答
 */
public class GetMessageAck extends JMQPayload implements BufAllocator, Releasable {
    // 存储的消息
    protected BrokerMessage[] messages;
    // 数据缓冲区
    protected RByteBuffer[] buffers;

    public GetMessageAck buffers(final RByteBuffer... buffers) {
        setBuffers(buffers);
        return this;
    }

    public GetMessageAck messages(final BrokerMessage[] messages) {
        this.messages = messages;
        return this;
    }

    public RByteBuffer[] getBuffers() {
        return this.buffers;
    }

    public void setBuffers(RByteBuffer[] buffers) {
        this.buffers = buffers;
    }

    public BrokerMessage[] getMessages() {
        return messages;
    }

    public void setMessages(BrokerMessage[] messages) {
        this.messages = messages;
    }

    public int predictionSize() {
        return 2 + 1;
    }

    protected int getSize(int begin, int end) {
        return super.getSize(begin, end) + getBufferSize();
    }

    public int getBufferSize() {
        int size = 0;

        if (buffers != null) {
            for (RByteBuffer buffer : buffers) {
                size += getBufferSize(buffer.getBuffer());
            }
        }

        return size;
    }

    protected int getBufferSize(ByteBuffer buffer) {
        if (buffer == null) {
            return 0;
        }
        return buffer.remaining();
    }

    @Override
    public void release() {
        if (buffers != null) {
            for (RByteBuffer buffer : buffers) {
                if (buffer != null) {
                    buffer.release();
                }
            }
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GetMessageAck{");
        if (buffers != null) {
            sb.append(", buffers=").append(Arrays.toString(buffers));
        } else if (messages != null) {
            sb.append(", messages=").append(Arrays.toString(messages));
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

        GetMessageAck that = (GetMessageAck) o;

        if (!Arrays.equals(buffers, that.buffers)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (buffers != null ? Arrays.hashCode(buffers) : 0);
        return result;
    }


    @Override
    public int type() {
        return CmdTypes.GET_MESSAGE_ACK;
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

        encodeBody(byteBuf);

        int end = byteBuf.writerIndex();
        byteBuf.writerIndex(begin);
        byteBuf.writeInt(getSize(begin, end));
        byteBuf.writerIndex(end);

        // 输出头部数据
        ctx.write(byteBuf, ctx.voidPromise());

        // 写入消息体
        if (buffers != null) {
            for (RByteBuffer buffer : buffers) {
                ctx.write(new WrappedByteBuf(buffer), ctx.voidPromise());
            }
        }
        return Unpooled.EMPTY_BUFFER;
    }

    protected void encodeBody(final ByteBuf out) throws Exception {
        // 2字节条数
        if (buffers == null) {
            out.writeShort(0);
            return;
        }
        out.writeShort(buffers.length);
    }

    private void encodeAttach(ByteBuf out, Object attach) throws Exception {
        JMQEncoder encoder = new JMQEncoder();
        encoder.encode((JMQHeader) attach, out);
    }
}