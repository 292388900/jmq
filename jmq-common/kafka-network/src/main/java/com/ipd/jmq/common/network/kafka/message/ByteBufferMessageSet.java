package com.ipd.jmq.common.network.kafka.message;

import com.ipd.jmq.common.network.kafka.exception.InvalidMessageException;
import com.ipd.jmq.common.network.kafka.model.CompressionCodec;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhangkepeng on 16-7-28.
 */
public class ByteBufferMessageSet {

    public static final int MESSAGE_SEZE_LENGTH = 4;
    public static final int OFFSET_LENGTH = 8;
    public static final int LOG_OVER_HEAD = MESSAGE_SEZE_LENGTH + OFFSET_LENGTH;

    private int shallowValidByteCount = -1;

    public static ByteBufferMessageSet emptySet = new ByteBufferMessageSet(ByteBuffer.allocate(0));

    private ByteBuffer buffer;

    public ByteBufferMessageSet(ByteBuffer buffer) {
        this.buffer = buffer;
        this.buffer.rewind();
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    /**
     * The total number of bytes in this message set, including any partial trailing messages
     */
    public int sizeInBytes() {
        return buffer.limit();
    }

    /**
     * Update the offsets for this message. This method attempts to do an in-place conversion
     * if there is no compression, but otherwise recopies the messages
     */
    public ByteBufferMessageSet assignOffsets(AtomicLong offset, CompressionCodec codec) throws IOException{
        if(codec == CompressionCodec.NoCompressionCodec) {
            // do an in-place conversion
            int position = 0;
            buffer.mark();
            while(position < sizeInBytes() - LOG_OVER_HEAD) {
                buffer.position(position);
                buffer.putLong(offset.getAndIncrement());
                position += LOG_OVER_HEAD + buffer.getInt();
            }
            buffer.reset();
            return this;
        } else {
            // messages are compressed, crack open the messageset and recompress with correct offset
            Iterator<KafkaMessageAndOffset> iterator = iterator(false);
            List<KafkaMessage> messages = new ArrayList<KafkaMessage>();
            while (iterator.hasNext()) {
                messages.add(iterator.next().getMessage());
            }
            return create(messages, codec, offset);
        }
    }

    /**
     * The size of a message set containing the given messages
     */
    public static int messageSetSize(List<KafkaMessage> messages) {
        int messageSetSize = 0;
        if (messages == null || messages.isEmpty()) {
            return messageSetSize;
        }
        for (KafkaMessage message : messages) {
            messageSetSize += entrySize(message);
        }
        return messageSetSize;
    }

    private ByteBufferMessageSet create(List<KafkaMessage> messages, CompressionCodec codec, AtomicLong offsetCounter) throws IOException{
        if (messages.size() == 0) {
            return emptySet;
        } else if (codec == CompressionCodec.NoCompressionCodec) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(messageSetSize(messages));
            for (KafkaMessage message : messages) {
                writeMessage(byteBuffer, message, offsetCounter.getAndIncrement());
            }
            byteBuffer.rewind();
            return new ByteBufferMessageSet(byteBuffer);
        } else {
            ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream(messageSetSize(messages));
            DataOutputStream output = new DataOutputStream(CompressionCodecFactory.apply(codec, byteArrayStream));
            long offset = -1L;
            try {
                for (KafkaMessage message : messages) {
                    offset = offsetCounter.getAndIncrement();
                    output.writeLong(offset);
                    output.writeInt(message.size());
                    output.write(message.buffer.array(), message.buffer.arrayOffset(), message.buffer.limit());
                }
            } finally {
                output.close();
            }
            byte[] bytes = byteArrayStream.toByteArray();
            KafkaMessage message = new KafkaMessage(bytes, codec);
            ByteBuffer byteBuffer = ByteBuffer.allocate(message.size() + LOG_OVER_HEAD);
            writeMessage(byteBuffer, message, offset);
            byteBuffer.rewind();
            return new ByteBufferMessageSet(byteBuffer);
        }
    }

    public static void writeMessage(ByteBuffer buffer, KafkaMessage message, long offset) {
        buffer.putLong(offset);
        buffer.putInt(message.size());
        buffer.put(message.buffer);
        message.buffer.rewind();
    }

    // 解析Kafka消息
    public Iterator<KafkaMessageAndOffset> iterator(boolean isShallow) {
        InternalIterator internalIterator = new InternalIterator(isShallow);
        Iterator<KafkaMessageAndOffset> iter = internalIterator.getIterator();
        return iter;
    }

    public enum State {
        DONE,
        READY,
        NOT_READY,
        FAILED
    }

    public interface Container {
        public Iterator getIterator();
    }

    public class InternalIterator implements Container {

        private boolean isShallow = false;

        public InternalIterator(boolean isShallow) {
            this.isShallow = isShallow;
        }

        @Override
        public Iterator getIterator() {
            return new IteratorTemplate();
        }

        private class IteratorTemplate implements Iterator<KafkaMessageAndOffset> {

            private Iterator<KafkaMessageAndOffset> innerIter = null;
            private ByteBuffer topIter = buffer.slice();
            private State state = State.NOT_READY;
            private KafkaMessageAndOffset nextItem = null;

            @Override
            public KafkaMessageAndOffset next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                state = State.NOT_READY;
                if (nextItem == null) {
                    throw new IllegalStateException("Expected item but none found.");
                }
                return nextItem;
            }

            @Override
            public boolean hasNext() {

                if (state == State.FAILED) {
                    throw new IllegalStateException("Iterator is in failed state");
                }

                switch (state) {
                    case DONE:
                        return false;
                    case READY:
                        return true;
                    default:
                        try {
                            boolean hasNext = maybeConputeNext();
                            return hasNext;
                        } catch (IOException e) {
                            return false;
                        }
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Removal not supported");
            }

            protected void resetState() {
                state = State.NOT_READY;
            }

            protected boolean maybeConputeNext() throws IOException{
                state = State.FAILED;
                nextItem = makeNext();
                if (state == State.DONE) {
                    return false;
                } else {
                    state = State.READY;
                    return true;
                }
            }

            protected KafkaMessageAndOffset allDone() {
                state = State.DONE;
                return null;
            }

            protected boolean innerDone() {
                return (innerIter == null || !innerIter.hasNext());
            }

            public KafkaMessageAndOffset makeNextOuter() throws IOException{
                // if there isn't at least an offset and size, we are done
                if (topIter.remaining() < 12) {
                    return allDone();
                }
                long offset = topIter.getLong();
                int size = topIter.getInt();
                if (size < KafkaMessage.minHeaderSize)
                    throw new InvalidMessageException("Message found with corrupt size (" + size + ")");

                // we have an incomplete message
                if (topIter.remaining() < size) {
                    return allDone();
                }

                // read the current message and check correctness
                ByteBuffer message = topIter.slice();
                message.limit(size);
                topIter.position(topIter.position() + size);
                KafkaMessage newMessage = new KafkaMessage(message);

                if (isShallow) {
                    return new KafkaMessageAndOffset(newMessage, offset);
                } else {
                    CompressionCodec codec = newMessage.compressionCodec();
                    switch (codec) {
                        case NoCompressionCodec:
                            innerIter = null;
                            return new KafkaMessageAndOffset(newMessage, offset);
                        default:
                            innerIter = ByteBufferMessageSet.decompress(newMessage).iterator(false);
                            if (!innerIter.hasNext()) {
                                innerIter = null;
                            }
                            return makeNext();
                    }
                }
            }

            private KafkaMessageAndOffset makeNext() throws IOException{
                if (isShallow) {
                    return makeNextOuter();
                } else {
                    if (innerDone()) {
                        return makeNextOuter();
                    } else {
                        return innerIter.next();
                    }
                }
            }
        }
    }

    /**
     * 解压消息体
     * @param message
     * @return
     * @throws IOException
     */
    public static ByteBufferMessageSet decompress(KafkaMessage message) throws IOException {
        ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(message.payload());
        byte[] intermediateBuffer = new byte[1024];
        InputStream compressed = CompressionCodecFactory.apply(message.compressionCodec(), inputStream);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        int count;
        try {
            while ((count = compressed.read(intermediateBuffer)) > 0) {
                outputStream.write(intermediateBuffer, 0, count);
            }
        } finally {
            compressed.close();
        }

        ByteBuffer outputBuffer = ByteBuffer.allocate(outputStream.size());
        outputBuffer.put(outputStream.toByteArray());
        outputBuffer.rewind();
        return new ByteBufferMessageSet(outputBuffer);
    }

    /**
     * The size of a size-delimited entry in a message set
     */
    public static int entrySize(KafkaMessage message) {
        return LOG_OVER_HEAD + message.size();
    }

    /**
     * 当满足下列条件时，表示两个Buffer相等：
     * 1、有相同的类型（byte、char、int等）
     * 2、Buffer中剩余的byte、char等的个数相等
     * 3、Buffer中所有剩余的byte、char等都相同
     */
    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        ByteBufferMessageSet byteBufferMessageSet = (ByteBufferMessageSet)object;

        if (buffer != null ? !buffer.equals(byteBufferMessageSet.buffer) : byteBufferMessageSet.buffer != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = buffer != null ? buffer.hashCode() : 0;
        return result;
    }

}
