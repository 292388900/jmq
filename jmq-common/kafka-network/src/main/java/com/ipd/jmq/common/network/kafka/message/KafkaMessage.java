package com.ipd.jmq.common.network.kafka.message;

import com.ipd.jmq.common.network.kafka.exception.InvalidMessageException;
import com.ipd.jmq.common.network.kafka.model.CompressionCodec;
import com.ipd.jmq.common.network.kafka.utils.Utils;

import java.nio.ByteBuffer;

/**
 * kafka 消息体
 */
public class KafkaMessage {

    /**
     * The current offset and size for all the fixed-length fields
     */
    public static int crcOffset = 0;
    public static int crcLength = 4;
    public static int magicOffset = crcOffset + crcLength;
    public static int magicLength = 1;
    public static int attributesOffset = magicOffset + magicLength;
    public static int attributesLength = 1;
    public static int keySizeOffset = attributesOffset + attributesLength;
    public static int keySizeLength = 4;
    public static int keyOffset = keySizeOffset + keySizeLength;
    public static int valueSizeLength = 4;

    /** The amount of overhead bytes in a message */
    public static int messageOverhead = keyOffset + valueSizeLength;

    /**
     * The minimum valid size for the message header
     */
    public static int minHeaderSize = crcLength + magicLength + attributesLength + keySizeLength + valueSizeLength;

    /**
     * The current "magic" value
     */
    public static byte currentMagicValue = 0;

    /**
     * Specifies the mask for the compression code. 3 bits to hold the compression codec.
     * 0 is reserved to indicate no compression
     */
    public static int compressionCodeMask = 0x07;

    /**
     * Compression code for uncompressed messages
     */
    public static int noCompression =  0;

    public ByteBuffer buffer;

    public KafkaMessage(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public KafkaMessage(byte[] bytes, byte[] key, CompressionCodec codec, int payloadOffset, int payloadSize) {

        int keyLength = 0;
        if (key != null) {
            keyLength = key.length;
        }

        int valueLength = 0;
        if (bytes == null) {
            valueLength = 0;
        } else if (bytes != null && payloadSize >= 0) {
            valueLength = payloadSize;
        } else {
            valueLength = bytes.length - payloadOffset;
        }
        this.buffer = ByteBuffer.allocate(KafkaMessage.crcLength + KafkaMessage.magicLength +
                KafkaMessage.attributesLength + KafkaMessage.keySizeLength + keyLength +
                KafkaMessage.valueSizeLength + valueLength);
        this.buffer.position(magicOffset);
        this.buffer.put(currentMagicValue);
        byte attributes = 0;
        if (codec.getCode() > 0) {
            attributes = (byte)(attributes | (compressionCodeMask & codec.getCode()));
        }
        this.buffer.put(attributes);
        if (key == null) {
            this.buffer.putInt(-1);
        } else {
            this.buffer.putInt(key.length);
            this.buffer.put(key, 0, key.length);
        }
        int size = 0;
        if (bytes == null) {
            size = -1;
        } else if (payloadSize >= 0) {
            size = payloadSize;
        } else {
            size = bytes.length - payloadOffset;
        }
        this.buffer.putInt(size);
        if(bytes != null)
            this.buffer.put(bytes, payloadOffset, size);
        this.buffer.rewind();
        // now compute the checksum and fill it in
        Utils.writeUnsignedInt(this.buffer, crcOffset, computeChecksum());
        this.buffer.rewind();
    }

    public KafkaMessage(byte[] bytes, byte[] key, CompressionCodec codec) {
        this(bytes, key, codec, 0, -1);
    }

    public KafkaMessage(byte[] bytes, CompressionCodec codec) {
        this(bytes, null, codec);
    }

    public KafkaMessage(byte[] bytes, byte[] key) {
        this(bytes, key, CompressionCodec.NoCompressionCodec);
    }

    public KafkaMessage(byte[] bytes) {
        this(bytes, null, CompressionCodec.NoCompressionCodec);
    }

    public long computeChecksum() {
        return Utils.crc32(buffer.array(), buffer.arrayOffset() + magicOffset, buffer.limit() - magicOffset);
    }

    public long checksum() {
        return Utils.readUnsignedInt(buffer, crcOffset);
    }

    public boolean isValid() {
        return checksum() == computeChecksum();
    }

    public void ensureValid() {
        if (!isValid()) {
            throw new InvalidMessageException("Message is corrupt (stored crc = " + checksum() + ", computed crc = " + computeChecksum() + ")");
        }
    }

    public int size() {
        return buffer.limit();
    }

    public int keySize() {
        return buffer.getInt(KafkaMessage.keySizeOffset);
    }

    public boolean hasKey() {
        return keySize() >= 0;
    }

    private int payloadSizeOffset() {
        int maxSize = Math.max(0, keySize());
        return KafkaMessage.keyOffset + maxSize;
    }

    public int payloadSize() {
        return buffer.getInt(payloadSizeOffset());
    }

    public boolean isNull() {
        return payloadSize() < 0;
    }

    public byte magic() {
        return buffer.get(magicOffset);
    }

    public byte attributes() {
        return buffer.get(attributesOffset);
    }

    public int compressionCodecType() {
        return buffer.get(attributesOffset) & compressionCodeMask;
    }

    public CompressionCodec compressionCodec() {
        return CompressionCodec.valueOf(buffer.get(attributesOffset) & compressionCodeMask);
    }

    public ByteBuffer payload() {
        return sliceDelimited(payloadSizeOffset());
    }

    public ByteBuffer key() {
        return sliceDelimited(keySizeOffset);
    }

    private ByteBuffer sliceDelimited(int start) {
        int size = buffer.getInt(start);
        if (size < 0) {
            return null;
        } else {
            ByteBuffer b = buffer.duplicate();
            b.position(start + 4);
            b = b.slice();
            b.limit(size);
            b.rewind();
            return b;
        }
    }

    @Override
    public String toString() {
        return String.format("Message(magic = %d, attributes = %d, crc = %d, key = %s, payload = %s)",magic(), attributes(), checksum(), key(), payload());
    }
}
