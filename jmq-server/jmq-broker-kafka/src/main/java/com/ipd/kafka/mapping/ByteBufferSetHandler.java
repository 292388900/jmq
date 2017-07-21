package com.ipd.kafka.mapping;

import com.ipd.jmq.common.network.kafka.exception.InvalidMessageSizeException;
import com.ipd.jmq.common.network.kafka.exception.MessageSizeTooLargeException;
import com.ipd.jmq.common.network.kafka.message.ByteBufferMessageSet;
import com.ipd.jmq.common.network.kafka.message.KafkaMessage;
import com.ipd.jmq.common.network.kafka.message.KafkaMessageAndOffset;
import com.ipd.jmq.common.network.kafka.model.CompressionCodec;
import com.ipd.jmq.common.network.kafka.model.KafkaLogAppendInfo;
import com.ipd.jmq.server.store.StoreConfig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Created by zhangkepeng on 16-12-8.
 */
public class ByteBufferSetHandler {

    // 存储配置
    private StoreConfig storeConfig;

    public ByteBufferSetHandler(StoreConfig storeConfig) {
        this.storeConfig = storeConfig;
    }

    public Iterator<KafkaMessageAndOffset> getALLKafakMessageAndOffsets(ByteBufferMessageSet byteBufferMessageSet, CompressionCodec codec) throws IOException{

        Iterator<KafkaMessageAndOffset> kafkaMessageAndOffsets = null;
        if(codec == CompressionCodec.NoCompressionCodec) {
            kafkaMessageAndOffsets = byteBufferMessageSet.iterator(true);
        } else {
            // messages are compressed, crack open the messageset and recompress with correct offset
            Iterator<KafkaMessageAndOffset> iterator = byteBufferMessageSet.iterator(false);
            kafkaMessageAndOffsets = iterator;
        }
        return kafkaMessageAndOffsets;
    }

    /**
     * Validate the following:
     * <ol>
     * <li> each message matches its CRC
     * <li> each message size is valid
     * </ol>
     *
     * Also compute the following quantities:
     * <ol>
     * <li> First offset in the message set
     * <li> Last offset in the message set
     * <li> Number of messages
     * <li> Number of valid bytes
     * <li> Whether the offsets are monotonically increasing
     * <li> Whether any compression codec is used (if many are used, then the last one is given)
     * </ol>
     */
    public KafkaLogAppendInfo analyzeAndValidateMessageSet(ByteBufferMessageSet messages) throws Exception{

        int shallowMessageCount = 0;
        int validBytesCount = 0;
        long firstOffset = -1L;
        long lastOffset = -1L;
        CompressionCodec codec = CompressionCodec.NoCompressionCodec;
        boolean monotonic = true;
        // 非压缩方式迭代遍历
        Iterator<KafkaMessageAndOffset> iterator = messages.iterator(true);
        while (iterator.hasNext()) {
            KafkaMessageAndOffset kafkaMessageAndOffset = iterator.next();
            // update the first offset if on the first message
            if (firstOffset < 0) {
                firstOffset = kafkaMessageAndOffset.getOffset();
            }
            // check that offsets are monotonically increasing
            if (lastOffset >= kafkaMessageAndOffset.getOffset()) {
                monotonic = false;
            }
            // update the last offset seen
            lastOffset = kafkaMessageAndOffset.getOffset();

            KafkaMessage m = kafkaMessageAndOffset.getMessage();

            // Check if the message sizes are valid.
            int messageSize = messages.entrySize(m);
            if (messageSize > storeConfig.getMaxMessageSize()) {
                throw new MessageSizeTooLargeException(String.format("Message size is %d bytes which exceeds the maximum configured message size of %d.", messageSize, storeConfig.getMaxMessageSize()));
            }

            // check the validity of the message by checking CRC
            m.ensureValid();
            shallowMessageCount += 1;
            validBytesCount += messageSize;
            CompressionCodec messageCodec = m.compressionCodec();
            if (messageCodec != CompressionCodec.NoCompressionCodec) {
                codec = messageCodec;
            }
        }
        KafkaLogAppendInfo kafkaLogAppendInfo = new KafkaLogAppendInfo(firstOffset, lastOffset, codec, shallowMessageCount, validBytesCount, monotonic);
        return kafkaLogAppendInfo;
    }

    /**
     * Trim any invalid bytes from the end of this message set (if there are any)
     * @param messages The message set to trim
     * @param kafkaLogAppendInfo The general information of the message set
     * @return A trimmed message set. This may be the same as what was passed in or it may not.
     */
    public ByteBufferMessageSet trimInvalidBytes(ByteBufferMessageSet messages, KafkaLogAppendInfo kafkaLogAppendInfo) throws Exception{
        int messageSetValidBytes = kafkaLogAppendInfo.getValidBytes();
        if(messageSetValidBytes < 0)
            throw new InvalidMessageSizeException("Illegal length of message set " + messageSetValidBytes + " Message set cannot be appended to log. Possible causes are corrupted produce requests");
        if(messageSetValidBytes == messages.sizeInBytes()) {
            return messages;
        } else {
            // trim invalid bytes
            ByteBuffer validByteBuffer = messages.getBuffer().duplicate();
            validByteBuffer.limit(messageSetValidBytes);
            return new ByteBufferMessageSet(validByteBuffer);
        }
    }
}
