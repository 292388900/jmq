package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.message.*;
import com.ipd.jmq.common.model.JournalLog;
import com.ipd.jmq.toolkit.io.Compressors;
import com.ipd.jmq.toolkit.io.Zip;
import com.ipd.jmq.toolkit.lang.Charsets;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

/**
 * 序列化工具类
 */
public class Serializer {
    protected static Logger logger = LoggerFactory.getLogger(Serializer.class);
    public static final byte BYTE_SIZE = 1;
    public static final byte SHORT_SIZE = 2;
    public static final byte INT_SIZE = 4;
    public static final byte LONG_SIZE = 8;
    public static final byte STRING_SIZE = 9;
    public static final char[] hexDigit =
            {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    /**
     * 读取字符串，字符长度<=255
     *
     * @param in 输入缓冲区
     * @return 字符串
     * @throws java.lang.Exception
     */
    public static String readString(final ByteBuf in) throws Exception {
        return readString(in, 1, false);
    }

    /**
     * 读取字符串
     *
     * @param in         输入缓冲区
     * @param lengthSize 长度大小
     * @return 字符串
     * @throws java.lang.Exception
     */
    public static String readString(final ByteBuf in, final int lengthSize) throws Exception {
        return readString(in, lengthSize, false);
    }

    /**
     * 读取字符串，前面有一个字符串长度字节
     *
     * @param in         输入缓冲区
     * @param lengthSize 长度大小
     * @param compressed 压缩标示
     * @return 字符串
     * @throws java.lang.Exception
     */
    public static String readString(final ByteBuf in, final int lengthSize, final boolean compressed) throws Exception {
        int length = 0;
        if (lengthSize == 1) {
            length = in.readUnsignedByte();
        } else if (lengthSize == 2) {
            length = in.readUnsignedShort();
        } else {
            length = in.readInt();
        }
        return read(in, length, compressed, "UTF-8");
    }

    /**
     * 读取字符串
     *
     * @param in         输入缓冲区
     * @param length     长度
     * @param compressed 压缩
     * @param charset    字符集
     * @return 字符串
     * @throws Exception
     */
    public static String read(final ByteBuf in, final int length, final boolean compressed, String charset) throws Exception {
        if (length <= 0) {
            return null;
        }
        byte[] bytes = readBytes(in, length);
        try {
            if (compressed) {
                bytes = Compressors.decompress(bytes, 0, bytes.length, Zip.INSTANCE);
            }

            if (charset == null || charset.isEmpty()) {
                charset = "UTF-8";
            }
            return new String(bytes, charset);
        } catch (UnsupportedEncodingException e) {
            return new String(bytes);
        }
    }

    /**
     * 读取字节数
     *
     * @param in     输入缓冲区
     * @param length 长度
     * @throws java.lang.Exception
     */
    public static byte[] readBytes(final ByteBuf in, final int length) throws Exception {
        if (in == null || length <= 0) {
            return new byte[0];
        }

        int len = in.readableBytes();
        if (len == 0) {
            return new byte[0];
        }
        if (length < len) {
            len = length;
        }

        byte[] bytes = new byte[len];
        in.readBytes(bytes);
        return bytes;

    }

    /**
     * 读取存储的消息
     *
     * @param in 输入缓冲区
     * @throws java.lang.Exception
     * @see Serializer#write(
     *com.ipd.jmq.common.message.BrokerMessage, io.netty.buffer.ByteBuf)
     */
    public static BrokerMessage readBrokerMessage(final ByteBuf in) throws Exception {
        if (in == null) {
            return null;
        }
        BrokerMessage message = new BrokerMessage();

        // 4个字节的消息长度
        message.setSize(in.readInt());
        // 2个字节的魔法标识
        int magic = in.readUnsignedShort();
        byte type = JournalLog.TYPE_MESSAGE;
        if (magic == BrokerMessage.MAGIC_LOG_CODE) {
            type = in.readByte();
        }
        message.setType(type);
        // 1个字节的系统字段 1-1：压缩标识 2-2：顺序消息 3-8：其他
        byte system = in.readByte();
        message.setCompressed((system & 0x1) == 1);
        // 1个字节消息顺序
        message.setOrdered((system >>> 1 & 0x1) == 1);
        // 1bit标识kafka消息
        message.setSource(((system >>> 2 & 0x1) == 1) ? BrokerMessage.MESSAGE_FROM_KAFKA : 0);
        if (message.isCompressed()) {
            // 1为Zip(默认), 0为Snappy
            message.setCompressionType(((system >>> 3 & 0x1) == 1) ? Message.CompressionType.Zip : Message.CompressionType.Snappy);
        }
        // 其它 暂时保留
        // 2字节业务标签
        message.setFlag(in.readShort());
        // 1字节优先级
        message.setPriority(in.readByte());
        // 8字节日志偏移
        message.setJournalOffset(in.readLong());
        // 1字节队列
        message.setQueueId(in.readUnsignedByte());
        // 8字节队列偏移
        message.setQueueOffset(in.readLong());
        // 6字节的客户端地址
        message.setClientAddress(readBytes(in, 6));
        // 6字节的服务端地址
        message.setServerAddress(readBytes(in, 6));
        // 8字节发送时间
        long sendTime = in.readLong();
        message.setSendTime(sendTime);
        // 4字节接受时间（相对发送时间的偏移）
        message.setReceiveTime(sendTime + in.readInt());
        // 4字节存储时间（相对发送时间偏移）
        message.setStoreTime(sendTime + in.readInt());
        // 8字节消息体CRC
        message.setBodyCRC(in.readLong());

        // 4字节消息体大小
        int length = in.readInt();
        if (length > 0) {
            // 消息体（字节数组）
            message.setBody(readByteBuffer(in, length));
        }
        // 1字节主题长度
        // 主题（字节数组）
        message.setTopic(readString(in));
        // 1字节应用长度
        // 应用（字节数组）
        message.setApp(readString(in));
        // 1字节业务ID长度
        // 业务ID（字节数组）
        message.setBusinessId(readString(in));
        if (isTxMessage(type)){
            //事物ID
            message.setTxId(readString(in, SHORT_SIZE));
        }
        // 2字节属性长度
        // 属性（字节数组）
        message.setAttributes(toMap(readString(in, 2)));

        return message;
    }

    private static boolean isTxMessage(byte type){
        return  type == JournalLog.TYPE_TX_PRE_MESSAGE || type == JournalLog.TYPE_TX_MESSAGE;
    }

    public static BrokerPrepare readBrokerPrepare(final ByteBuf in) throws Exception {
        if (in == null) {
            return null;
        }

        BrokerPrepare prepare = new BrokerPrepare();
        prepare.setSize(in.readInt());
        //跳过2字节魔术字符
        in.readShort();
        //跳过1字节类型
        in.readByte();
        //8字节日志偏移
        prepare.setJournalOffset(in.readLong());
        //8字节存储时间
        prepare.setStoreTime(in.readLong());
        //1字节主题
        prepare.setTopic(Serializer.readString(in));
        //2字节事务ID
        prepare.setTxId(Serializer.readString(in, SHORT_SIZE));
        //2字节事务查询ID
        prepare.setQueryId(Serializer.readString(in, SHORT_SIZE));
        //8字节事务开始时间
        prepare.setStartTime(in.readLong());
        //4字节事务超时时间
        prepare.setTimeout(in.readInt());
        //1字节事务超时动作
        prepare.setTimeoutAction(in.readByte());
        //n字节属性
        prepare.setAttrs(Serializer.readMap(in));
        //8字节校验和
        prepare.setCrc(in.readLong());
        return prepare;
    }

    public static BrokerCommit readBrokerCommit(final ByteBuf in) throws Exception {
        BrokerCommit commit = new BrokerCommit();
        commit.setSize(in.readInt());
        //跳过2字节魔术字符
        in.readShort();
        //跳过1字节类型
        in.readByte();
        //8字节日志偏移
        commit.setJournalOffset(in.readLong());
        //8字节存储时间
        commit.setStoreTime(in.readLong());
        //1字节主题
        commit.setTopic(Serializer.readString(in));
        //2字节事务ID
        commit.setTxId(Serializer.readString(in, SHORT_SIZE));
        //n字节属性
        commit.setAttrs(Serializer.readMap(in));
        //4字节消息数量
        commit.setMsgCount(in.readInt());
        //8字节校验和
        commit.setCrc(in.readLong());
        return commit;
    }

    public static BrokerRollback readBrokerRollback(final ByteBuf in) throws Exception {
        BrokerRollback rollback = new BrokerRollback();
        rollback.setSize(in.readInt());
        //跳过2字节魔术字符
        in.readShort();
        //跳过1字节类型
        in.readByte();
        //8字节日志偏移
        rollback.setJournalOffset(in.readLong());
        //8字节存储时间
        rollback.setStoreTime(in.readLong());
        //1字节主题
        rollback.setTopic(Serializer.readString(in));
        //2字节事务ID
        rollback.setTxId(Serializer.readString(in, SHORT_SIZE));
        //n字节属性
        rollback.setAttrs(Serializer.readMap(in));
        //8字节校验和
        rollback.setCrc(in.readLong());
        return rollback;
    }

    public static BrokerRefMessage readBrokerRefMessage(final ByteBuf in) throws Exception {
        BrokerRefMessage refMessage = new BrokerRefMessage();
        refMessage.setSize(in.readInt());
        //跳过2字节魔术字符
        in.readShort();
        //跳过1字节类型
        in.readByte();
        //2字节自队列ID
        refMessage.setQueueId(in.readShort());
        //8字节队列偏移
        refMessage.setQueueOffset(in.readLong());
        //8字节日志偏移
        refMessage.setJournalOffset(in.readLong());
        //8字节存储时间
        refMessage.setStoreTime(in.readLong());
        //4字节引用日志长度
        refMessage.setRefJournalSize(in.readInt());
        //8字节引用日志偏移
        refMessage.setRefJournalOffset(in.readLong());
        //n字节属性
        refMessage.setAttrs(Serializer.readMap(in));
        //1字节主题
        refMessage.setTopic(Serializer.readString(in));
        //2字节标示
        refMessage.setFlag(in.readShort());
        //8字节校验和
        refMessage.setCrc(in.readLong());
        return refMessage;
    }

    /**
     * 读取存储消息
     *
     * @param in 输入缓冲区
     * @return 存储消息
     * @throws java.lang.Exception
     * @see Serializer#write(
     *com.ipd.jmq.common.message.Message, io.netty.buffer.ByteBuf)
     */
    public static BrokerMessage readMessage(final ByteBuf in) throws Exception {
        if (in == null) {
            return null;
        }

        BrokerMessage message = new BrokerMessage();
        // 1字节系统字段 1-1:压缩标识 2-2:顺序消息 3-8:其它
        short sysCode = in.readUnsignedByte();
        message.setCompressed((sysCode & 0x1) == 1);
        // 1个字节消息顺序
        message.setOrdered(((sysCode >> 1) & 0x1) == 1);
        // 1bit标识kafka消息
        message.setSource(((sysCode >> 2 & 0x1) == 1) ? BrokerMessage.MESSAGE_FROM_KAFKA : 0);
        if (message.isCompressed()) {
            // 1为Zip(默认), 0为Snappy
            message.setCompressionType(((sysCode >> 3 & 0x1) == 1) ? Message.CompressionType.Zip : Message.CompressionType.Snappy);
        }

        message.setFlag(in.readShort());
        // 1字节优先级
        message.setPriority(in.readByte());
        // 4字节消息体CRC
        message.setBodyCRC(in.readLong());
        // 发送时间。
        message.setSendTime(in.readLong());

        // 4字节消息体大小
        // 消息体
        int length = in.readInt();
        if (length > 0) {
            // 消息体（字节数组）
            message.setBody(readByteBuffer(in, length));
        }

        // 1字节主题长度
        // 主题
        message.setTopic(readString(in, 1));
        // 1字节应用长度
        // 应用
        message.setApp(readString(in, 1));
        // 1字节业务ID长度
        // 业务ID
        message.setBusinessId(readString(in, 1));
        // 2字节属性长度
        // 属性 （以属性文件格式存储）
        message.setAttributes(toMap(readString(in, 2)));

        return message;
    }

    /**
     * 读取存储消息数组
     *
     * @param in 输入缓冲区
     * @return 存储消息数组
     * @throws java.lang.Exception
     * @see Serializer#write(
     *com.ipd.jmq.common.message.Message[], io.netty.buffer.ByteBuf)
     */
    public static BrokerMessage[] readMessages(final ByteBuf in) throws Exception {
        if (in == null) {
            return null;
        }
        // 2字节消息条数
        int count = in.readShort();
        if (count < 1) { //没有消息
            return null;
        }

        BrokerMessage[] messages = new BrokerMessage[count];
        for (int i = 0; i < count; i++) {
            messages[i] = readMessage(in);
        }

        return messages;
    }

    /**
     * 解码应答消息体中的消息位置数组
     *
     * @param in 输入缓冲区
     * @return 消息位置数组
     * @throws java.lang.Exception
     * @see com.ipd.jmq.common.network.v3.codec.encode# encodeBody(io.netty.buffer.ByteBuf)
     * @see Serializer#write(com.ipd.jmq.common.message.MessageLocation[],
     * io.netty.buffer.ByteBuf)
     */
    public static MessageLocation[] readMessageLocations(final ByteBuf in) throws Exception {
        if (in == null) {
            return new MessageLocation[0];
        }
        // 6字节服务地址
        byte[] address = Serializer.readBytes(in, 6);
        // 1字节主题长度
        String topic = Serializer.readString(in);

        // 2字节条数
        int length = in.readUnsignedShort();

        MessageLocation[] locations = new MessageLocation[length];
        short queueId;
        long queueOffset;
        for (int i = 0; i < length; i++) {
            // 1字节队列ID
            queueId = in.readUnsignedByte();
            // 8字节队列偏移
            queueOffset = in.readLong();

            locations[i] = new MessageLocation(address, topic, queueId, queueOffset);
        }

        return locations;
    }

    public static long readSendTime(final ByteBuffer in) throws Exception {
        if (in == null) {
            return 0;
        }
        in.mark();
        try {
            //消息长度+magicCode+systemCode+flag+priority+journalOffset+queueId+queueOffset+remoteAddress+serverAddress
            int index = 4 + 2 + 1 + 2 + 1 + 8 + 1 + 8 + 6 + 6;
            return in.getLong(index);
        } finally {
            in.reset();
        }
    }

    //读取BrokerMessage发送时间
    public static long readMessageSendTime(final ByteBuffer in) throws Exception {
        if (in == null) {
            return 0;
        }
        in.mark();
        try {
            //消息长度+magicCode+systemCode+flag+priority+journalOffset+queueId+queueOffset+remoteAddress+serverAddress
            int index = 4 + 2 + 1 + 1 + 2 + 1 + 8 + 1 + 8 + 6 + 6;
            return in.getLong(index);
        } finally {
            in.reset();
        }
    }

    //读取BrokerMessage存储时间
    public static long readMessageStoreTime(final ByteBuffer in) throws Exception {
        if (in == null) {
            return 0;
        }
        in.mark();
        try {
            //消息长度+magicCode+systemCode+flag+priority+journalOffset+queueId+queueOffset+remoteAddress+serverAddress
            int index = 4 + 2 + 1 + 2 + 1 + 8 + 1 + 8 + 6 + 6;
            //发送时间
            long sendTime = in.getLong(index);
            //发送时间+存储时间偏移
            return sendTime + in.getInt(index + 8 + 4);
        } finally {
            in.reset();
        }
    }

    //读取JournalLog魔术字段
    public static short readMessageMagicCode(final ByteBuffer in) {
        if (in == null) {
            return BrokerMessage.MAGIC_CODE;
        }
        in.mark();
        try {
            return in.getShort(4);
        } finally {
            in.reset();
        }
    }

    //读取JournalLog类型字段
    public static byte readMessageTypeCode(final ByteBuffer in) {
        if (in == null) {
            return BrokerMessage.TYPE_REF_MESSAGE;
        }
        in.mark();
        try {
            return in.get(4 + 2);
        } finally {
            in.reset();
        }
    }

    //重置BrokerMessage的queueId和JournalOffset
    public static void resetMessageLocationAndStoreTime(final ByteBuffer in, long journalOffset, byte queueId, long
            queueOffset, int storeTimeOffset) {
        if (in == null) {
            return;
        }
        in.mark();
        try {
            in.putLong(4 + 2 + 1 + 1 + 2 + 1, journalOffset);
            in.put(4 + 2 + 1 + 1 + 2 + 1 + 8, queueId);
            in.putLong(4 + 2 + 1 + 1 + 2 + 1 + 8 + 1, queueOffset);
            in.putInt(4 + 2 + 1 + 1 + 2 + 1 + 8 + 1 + 8 + 6 + 6 + 8 + 4, storeTimeOffset);
        } finally {
            in.reset();
        }
    }

    /**
     * 写入存储消息
     *
     * @param message 存储消息
     * @param out     输出缓冲区
     * @throws java.lang.Exception
     * @see Serializer#readBrokerMessage(io.netty.buffer.ByteBuf)
     */
    public static void write(final BrokerMessage message, final ByteBuf out) throws Exception {
        int size;
        if (out == null || message == null) {
            return;
        }
        // 记录写入的起始位置
        int begin = out.writerIndex();
        // 4个字节的消息长度需要计算出来
        out.writeInt(0);
        // 2个字节的魔法标识
        if (isTxMessage(message.getType())) {
            out.writeShort(BrokerMessage.MAGIC_LOG_CODE);
            out.writeByte(message.getType());
        } else {
            out.writeShort(BrokerMessage.MAGIC_CODE);
        }

        // 1个字节的系统字段 1-1：压缩标识 2-2：顺序消息 3-3: Kafka消息 4-8：其他,预留未用
        byte sysCode = (byte) (message.isCompressed() ? 1 : 0);
        sysCode |= ((message.isOrdered() ? 1 : 0) << 1) & 0x3;
        sysCode |= (((message.getSource() == BrokerMessage.MESSAGE_FROM_KAFKA) ? 1 : 0) << 2) & 0x7;
        // compressor
        if (message.isCompressed()) {
            sysCode |= (((message.getCompressionType() != Message.CompressionType.Snappy) ? 1 : 0) << 3) & 0x9;
        }
        out.writeByte(sysCode);
        // 2字节业务标签
        out.writeShort(message.getFlag());
        // 1字节优先级
        out.writeByte(message.getPriority());
        // 8字节日志偏移
        out.writeLong(message.getJournalOffset());
        // 1字节队列
        out.writeByte(message.getQueueId());
        // 8字节队列偏移
        out.writeLong(message.getQueueOffset());
        // 6字节的客户端地址
        out.writeBytes(message.getClientAddress() == null ? new byte[6] : message.getClientAddress());
        // 6字节的服务端地址
        out.writeBytes(message.getServerAddress() == null ? new byte[6] : message.getServerAddress());
        // 8字节发送时间
        out.writeLong(message.getSendTime());
        // 4字节接受时间（相对发送时间的偏移）
        out.writeInt((int) (message.
                getReceiveTime() - message.getSendTime()));
        // 4字节存储时间（相对发送时间偏移）
        out.writeInt((int) (message.
                getStoreTime() - message.getSendTime()));
        // 8字节消息体CRC
        out.writeLong(message.getBodyCRC());

        // 4字节消息体大小
        // 消息体（字节数组）
        write(message.getBody(), out);

        // 1字节主题长度
        // 主题（字节数组）
        write(message.getTopic(), out);
        // 1字节应用长度
        // 应用（字节数组）
        write(message.getApp(), out);
        // 1字节业务ID长度
        // 业务ID（字节数组）
        write(message.getBusinessId(), out);
        if (isTxMessage(message.getType())) {
            //写入事物ID
            write(message.getTxId(), out, SHORT_SIZE);
        }
        // 2字节属性长度
        // 属性（字节数组）
        write(toProperties(message.getAttributes()), out, 2);

        // 重写总长度
        int end = out.writerIndex();
        size = end - begin;
        message.setSize(size);
        out.writerIndex(begin);
        out.writeInt(size);
        out.writerIndex(end);
    }

    public static void write(final BrokerRefMessage message, final ByteBuf out) throws Exception {
        int size;
        if (out == null || message == null) {
            return;
        }

        // 记录写入的起始位置
        int begin = out.writerIndex();
        // 4个字节的消息长度需要计算出来
        out.writeInt(0);
        // 2个字节的魔法标识
        out.writeShort(BrokerMessage.MAGIC_LOG_CODE);
        //类型
        out.writeByte(message.getType());
        out.writeShort(message.getQueueId());
        out.writeLong(message.getQueueOffset());
        out.writeLong(message.getJournalOffset());
        out.writeLong(message.getStoreTime());
        //指向的日志长度
        out.writeInt(message.getRefJournalSize());
        //指向的日志位置
        out.writeLong(message.getRefJournalOffset());
        //写入内置属性
        write(message.getAttrs(), out);
        //写入TOPIC
        write(message.getTopic(), out);
        //写入flag
        out.writeShort(message.getFlag());
        //校验码
        out.writeLong(message.getCrc());
        // 重写总长度
        int end = out.writerIndex();
        size = end - begin;
        message.setSize(size);
        out.writerIndex(begin);
        out.writeInt(size);
        out.writerIndex(end);
    }

    /**
     * 写入存储消息
     *
     * @param messages 存储消息
     * @param out      输出缓冲区
     * @throws java.lang.Exception
     */
    public static void write(final BrokerMessage[] messages, final ByteBuf out) throws Exception {
        if (out == null) {
            return;
        }

        int count = messages == null ? 0 : messages.length;
        out.writeShort(count);

        for (int i = 0; i < count; i++) {
            write(messages[i], out);
        }
    }

    /**
     * 写消息
     *
     * @param message 消息
     * @param out     输出缓冲区
     * @throws java.lang.Exception
     * @see Serializer#readMessage(io.netty.buffer.ByteBuf)
     */
    public static void write(final Message message, final ByteBuf out) throws Exception {
        if (out == null || message == null) {
            return;
        }

        // 1字节系统字段 1-1:消息体压缩标识 2-2:顺序消息 3-3:属性压缩标示 4-8:其它
        short sysCode = (short) (message.isCompressed() ? 1 : 0);
        sysCode |= ((message.isOrdered() ? 1 : 0) << 1) & 0x3;
        // Message cannot be cast to BrokerMessage here
        //sysCode |= (((((BrokerMessage) message).getSource() == BrokerMessage.MESSAGE_FROM_KAFKA) ? 1 : 0) << 2) & 0x7;
        if (message.isCompressed()) {
            sysCode |= (((message.getCompressionType() != Message.CompressionType.Snappy) ? 1 : 0) << 3) & 0x9;
        }
        out.writeByte(sysCode);

        // 2字节业务标签
        out.writeShort(message.getFlag());
        // 1字节优先级
        out.writeByte(message.getPriority());
        // 4字节消息体CRC
        out.writeLong(message.getBodyCRC());
        // 发送时间
        out.writeLong(message.getSendTime());
        // 4字节消息体大小
        // 消息体
        write(message.getBody(), out);
        // 1字节主题长度
        // 主题
        write(message.getTopic(), out);
        // 1字节应用长度
        // 应用
        write(message.getApp(), out);

        // 1字节业务ID长度
        // 业务ID
        write(message.getBusinessId(), out);
        // 2字节属性长度
        // 属性 （以属性文件格式存储）
        write(toProperties(message.getAttributes()), out, 2);
    }

    /**
     * 写入消息
     *
     * @param messages 消息数组
     * @param out      输出缓冲区
     * @throws java.lang.Exception
     * @see Serializer#readMessages(io.netty.buffer.ByteBuf)
     */
    public static void write(final Message[] messages, final ByteBuf out) throws Exception {
        if (out == null) {
            return;
        }

        int count = messages == null ? 0 : messages.length;

        // 2字节消息个数
        out.writeShort(count);

        if (count > 0) {
            for (Message message : messages) {
                write(message, out);
            }
        }
    }

    /**
     * 写消息位置数组
     *
     * @param locations 消息位置数组
     * @param out       输出缓冲区
     * @throws java.lang.Exception
     * @see AckMessage# encodeBody(io.netty.buffer.ByteBuf)
     * @see Serializer#readMessageLocations(io.netty.buffer.ByteBuf)
     */
    public static void write(final MessageLocation[] locations, final ByteBuf out) throws Exception {
        if (out == null) {
            return;
        }
        if (locations == null || locations.length < 1) {
            throw new IllegalArgumentException("locations is null or empty");
        }

        String topic = locations[0].getTopic();
        byte[] address = locations[0].getAddress();

        // 6字节服务地址
        out.writeBytes(address);
        // 1字节主题长度
        write(topic, out, 1);
        // 2字节条数
        out.writeShort(locations.length);

        for (MessageLocation location : locations) {
            //1字节队列ID
            out.writeByte(location.getQueueId());
            // 8字节队列偏移
            out.writeLong(location.getQueueOffset());
        }
    }

    /**
     * 写整数
     *
     * @param value      整数
     * @param out        输出
     * @param lengthSize 长度字节数
     * @throws java.lang.Exception
     */
    public static void write(final int value, final ByteBuf out, final int lengthSize) throws Exception {
        if (out == null) {
            return;
        }
        switch (lengthSize) {
            case BYTE_SIZE:
                out.writeByte(value);
                break;
            case SHORT_SIZE:
                out.writeShort(value);
                break;
            case INT_SIZE:
                out.writeInt(value);
                break;
        }
    }

    /**
     * 写长整数
     *
     * @param value 长整数
     * @param out   输出
     * @throws java.lang.Exception
     */
    public static void write(final long value, final ByteBuf out) throws Exception {
        if (out == null) {
            return;
        }
        out.writeLong(value);
    }

    /**
     * 写字符串(长度<=255)
     *
     * @param value 字符串
     * @param out   输出缓冲区
     * @throws java.lang.Exception
     */
    public static void write(final String value, final ByteBuf out) throws Exception {
        write(value, out, 1, false);
    }

    /**
     * 获取字节数组
     *
     * @param value   字符串
     * @param charset 字符集
     * @return 字节数组
     */
    protected static byte[] getBytes(final String value, final Charset charset) {
        if (value == null) {
            return new byte[0];
        }

        byte[] bytes;
        if (charset == null) {
            bytes = value.getBytes(Charsets.UTF_8);
        } else {
            bytes = value.getBytes(charset);
        }
        return bytes;
    }

    /**
     * 预测字符串占用的字节数
     *
     * @param value      值
     * @param lengthSize 长度字节数
     * @return 字节数
     */
    public static int getPredictionSize(final String value, final int lengthSize) {
        int size = lengthSize;
        if (value != null) {
            // 按照UNICODE计算字节数
            size += value.length() * 2;
        }
        return size;
    }

    /**
     * 预测字符串占用的字节数，字符串长度占用1位
     *
     * @param values 值
     * @return 字节数
     */
    public static int getPredictionSize(final String... values) {
        int size = 0;
        if (values != null) {
            for (String value : values) {
                size++;
                if (value != null) {
                    // 按照UNICODE计算字节数
                    size += value.length() * 2;
                }
            }
        }
        return size;
    }

    /**
     * 预测消息占用的字节数
     *
     * @param messages 消息数组
     * @return 字节数
     */
    public static int getPredictionSize(final BrokerMessage... messages) {
        int size = 2;
        if (messages != null) {
            for (BrokerMessage message : messages) {
                size += 4 + 2 + 1 + 2 + 1 + 8 + 1 + 8 + 6 + 6 + 8 + 4 + 4 + 8;
                size += getPredictionSize(message.getBody(), true);
                size += getPredictionSize(message.getTopic(), message.getApp(), message.getBusinessId());
                size += 2;
                if (message.getAttributes() != null && !message.getAttributes().isEmpty()) {
                    size += 100;
                }
            }
        }

        return size;
    }

    /**
     * 预测消息占用的字节数
     *
     * @param messages 消息数组
     * @return 字节数
     */
    public static int getPredictionSize(final Message... messages) {
        int size = 2;
        if (messages != null) {
            for (Message message : messages) {
                size += 1 + 2 + 1 + 8 + 8;
                size += getPredictionSize(message.getBody(), true);
                size += Serializer.getPredictionSize(message.getTopic(), message.getApp(), message.getBusinessId());
                size += 2;
                if (message.getAttributes() != null && !message.getAttributes().isEmpty()) {
                    size += 100;
                }
            }
        }

        return size;
    }

    /**
     * 预测缓冲区占用的字节数
     *
     * @param buffer      缓冲区
     * @param writeLength 写长度标示
     * @return 字节数
     */
    public static int getPredictionSize(final ByteBuffer buffer, final boolean writeLength) {
        int size = 0;
        if (writeLength) {
            size += 4;
        }
        if (buffer != null) {
            size += buffer.remaining();
        }
        return size;
    }

    /**
     * 预测消息位置占用的字节数
     *
     * @param locations 消息位置数组
     * @return 字节数
     */
    public static int getPredictionSize(final MessageLocation... locations) {
        String topic = locations[0].getTopic();
        return 6 + getPredictionSize(topic) + 2 + locations.length * 9;
    }

    /**
     * 预测map对象占用的字节数
     *
     * @param hashMap
     * @return
     */
    public static int getPredictionSize(final Map<Object, Object> hashMap) {
        int count = 0;
        int size;
        if (hashMap == null) {
            return count;
        }
        size = hashMap.size();
        if (0 == size) {
            count = 4;
            return count;
        }
        count += 4;
        Iterator iterator = hashMap.entrySet().iterator();
        Map.Entry entry;
        while (iterator.hasNext()) {
            entry = (Map.Entry) iterator.next();
            try {
                count += getMapObjectSize(entry.getKey());
                count += getMapObjectSize(entry.getValue());
            } catch (Exception e) {
                logger.error("predictionSize exception, may type error", e);
            }
        }
        return count;
    }

    /**
     * 预测Map对应的Object占用的字节数
     *
     * @param object
     * @return
     * @throws Exception
     */
    public static int getMapObjectSize(Object object) throws Exception {
        if (object instanceof Byte) {
            return 2;
        } else if (object instanceof Short) {
            return 3;
        } else if (object instanceof Integer) {
            return 5;
        } else if (object instanceof Long) {
            return 9;
        } else if (object instanceof Double) {
            return 9;
        } else if (object instanceof String) {
            return 1 + getPredictionSize((String) object);
        } else {
            throw new Exception();
        }
    }

    /**
     * 写字符串
     *
     * @param value      字符串
     * @param out        输出缓冲区
     * @param lengthSize 长度字节数
     * @throws java.lang.Exception
     */
    public static void write(final String value, final ByteBuf out, final int lengthSize) throws Exception {
        write(value, out, lengthSize, false);
    }

    /**
     * 写字符串
     *
     * @param value      字符串
     * @param out        输出缓冲区
     * @param lengthSize 长度字节数
     * @param compressed 是否进行压缩
     * @throws java.lang.Exception
     */
    public static void write(final String value, final ByteBuf out, final int lengthSize,
                             final boolean compressed) throws Exception {
        if (out == null) {
            return;
        }
        if (value != null && !value.isEmpty()) {
            byte[] bytes = getBytes(value, Charsets.UTF_8);
            if (compressed) {
                bytes = Compressors.compress(bytes, 0, bytes.length, Zip.INSTANCE);
            }
            write(bytes.length, out, lengthSize);
            out.writeBytes(bytes);
        } else {
            write(0, out, lengthSize);
        }
    }

    /**
     * 写入字符串
     *
     * @param value   字符串
     * @param out     输出缓冲区
     * @param charset 字符集
     * @throws Exception
     */
    public static void write(final String value, final ByteBuf out, final Charset charset) throws Exception {
        if (out == null || value == null) {
            return;
        }
        out.writeBytes(getBytes(value, charset));
    }


    /**
     * 写数据
     *
     * @param value 数据源
     * @param out   输出缓冲区
     * @throws java.lang.Exception
     */
    public static void write(final ByteBuffer value, final ByteBuf out) throws Exception {
        write(value, out, true);
    }

    /**
     * 写数据
     *
     * @param value       数据源
     * @param out         输出缓冲区
     * @param writeLength 是否写长度
     * @throws java.lang.Exception
     */
    public static void write(final ByteBuffer value, final ByteBuf out, final boolean writeLength) throws Exception {
        int length = value == null ? 0 : value.remaining();
        if (writeLength) {
            out.writeInt(length);
        }
        if (length > 0) {
            if (value.hasArray()) {
                out.writeBytes(value.array(), value.arrayOffset() + value.position(), value.remaining());
            } else {
                out.writeBytes(value.slice());
            }
        }
    }

    /**
     * 读数据
     *
     * @param in 输入缓冲区
     * @return 缓冲区
     * @throws java.lang.Exception
     */
    public static ByteBuffer readByteBuffer(final ByteBuf in) throws Exception {
        if (in == null) {
            return null;
        }

        int length = in.readInt();
        if (length <= 0) {
            return null;
        }

        return readByteBuffer(in, length);
    }

    /**
     * 读数据
     *
     * @param in     输入缓冲区
     * @param length 读取长度
     * @return 缓冲区
     * @throws java.lang.Exception
     */
    public static ByteBuffer readByteBuffer(final ByteBuf in, int length) throws Exception {
        if (in == null || length <= 0) {
            return null;
        }

        ByteBuffer buffer;

        if (in.isDirect()) {
            buffer = ByteBuffer.allocate(length);
            in.readBytes(buffer);
            buffer.flip();
        } else {
            int index = in.readerIndex();
            buffer = in.slice(index, length).nioBuffer();
            in.readerIndex(index + length);
        }

        return buffer;
    }

    /**
     * 把Properties字符串转换成Map
     *
     * @param text 字符串
     * @return 散列对象
     * @throws IOException
     */
    protected static Map<String, String> toMap(final String text) throws Exception {
        if (text == null || text.isEmpty()) {
            return null;
        }
        Properties properties = new Properties();
        properties.load(new StringReader(text));

        return (Map<String, String>) new HashMap(properties);
    }

    /**
     * 把Map转换成Properties字符串
     *
     * @param attributes 散列
     * @return 字符串
     */
    protected static String toProperties(final Map<String, String> attributes) {
        if (attributes == null) {
            return "";
        }
        int count = 0;
        StringBuilder builder = new StringBuilder(100);
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            if (count > 0) {
                builder.append('\n');
            }
            append(builder, entry.getKey(), true, true);
            builder.append('=');
            append(builder, entry.getValue(), false, true);
            count++;
        }
        return builder.toString();
    }

    /**
     * 添加字符串
     *
     * @param builder       缓冲区
     * @param value         字符串
     * @param escapeSpace   转移空格标示
     * @param escapeUnicode 转移Unicode标示
     */
    private static void append(final StringBuilder builder, final String value, final boolean escapeSpace,
                               final boolean escapeUnicode) {
        int len = value.length();
        for (int x = 0; x < len; x++) {
            char aChar = value.charAt(x);
            // Handle common case first, selecting largest block that
            // avoids the specials below
            if ((aChar > 61) && (aChar < 127)) {
                if (aChar == '\\') {
                    builder.append('\\');
                    builder.append('\\');
                    continue;
                }
                builder.append(aChar);
                continue;
            }
            switch (aChar) {
                case ' ':
                    if (x == 0 || escapeSpace) {
                        builder.append('\\');
                    }
                    builder.append(' ');
                    break;
                case '\t':
                    builder.append('\\');
                    builder.append('t');
                    break;
                case '\n':
                    builder.append('\\');
                    builder.append('n');
                    break;
                case '\r':
                    builder.append('\\');
                    builder.append('r');
                    break;
                case '\f':
                    builder.append('\\');
                    builder.append('f');
                    break;
                case '=': // Fall through
                case ':': // Fall through
                case '#': // Fall through
                case '!':
                    builder.append('\\');
                    builder.append(aChar);
                    break;
                default:
                    if (((aChar < 0x0020) || (aChar > 0x007e)) & escapeUnicode) {
                        builder.append('\\');
                        builder.append('u');
                        builder.append(hexDigit[((aChar >> 12) & 0xF)]);
                        builder.append(hexDigit[((aChar >> 8) & 0xF)]);
                        builder.append(hexDigit[((aChar >> 4) & 0xF)]);
                        builder.append(hexDigit[(aChar & 0xF)]);
                    } else {
                        builder.append(aChar);
                    }
            }
        }
    }

    /**
     * 读取Map数据
     *
     * @param in
     * @return
     * @throws Exception
     */
    public static Map<Object, Object> readMap(final ByteBuf in) throws Exception {
        return JMQMapTools.readMap(in);
    }


    /**
     * 写入map数据
     *
     * @param hashMap
     * @param out
     * @param <K>
     * @param <V>
     * @throws Exception
     */
    public static <K, V> void write(final Map<K, V> hashMap, ByteBuf out) throws Exception {
        JMQMapTools.write(hashMap, out);
    }


    public static ByteBuf serialize(JournalLog log, ByteBuf out) throws Exception {
        switch (log.getType()) {
            case JournalLog.TYPE_MESSAGE:
            case JournalLog.TYPE_TX_PRE_MESSAGE:
                BrokerMessage message = (BrokerMessage) log;
                write(message, out);
                break;
            case JournalLog.TYPE_REF_MESSAGE:
                BrokerRefMessage refMessage = (BrokerRefMessage) log;
                write(refMessage, out);
                break;
            case JournalLog.TYPE_TX_PREPARE:
                BrokerPrepare prepare = (BrokerPrepare) log;
                write(prepare, out);
                break;
            case JournalLog.TYPE_TX_COMMIT:
                BrokerCommit commit = (BrokerCommit) log;
                write(commit, out);
                break;
            case JournalLog.TYPE_TX_ROLLBACK:
                BrokerRollback rollback = (BrokerRollback) log;
                write(rollback, out);
                break;
            default:
                throw new RuntimeException("invalid type:" + log.getType());
        }
        return out;
    }

    public static ByteBuf write(BrokerPrepare prepare, ByteBuf out) throws Exception {
        /**
         * size+MAGIC+type
         */
        int begin = out.writerIndex();
        //长度占位
        out.writeInt(0);
        //魔术字符
        out.writeShort(BrokerMessage.MAGIC_LOG_CODE);
        //命令类型
        out.writeByte(prepare.getType());
        //日志位置
        out.writeLong(prepare.getJournalOffset());
        //存储时间
        out.writeLong(prepare.getStoreTime());
        //主题
        write(prepare.getTopic(), out);
        //事物ID
        write(prepare.getTxId(), out, SHORT_SIZE);
        //查询标识
        write(prepare.getQueryId(), out, SHORT_SIZE);
        //事务开启时间
        out.writeLong(prepare.getStartTime());
        //事务超时时间
        out.writeInt(prepare.getTimeout());
        //事务超时时间
        out.writeByte(prepare.getTimeoutAction());
        //扩展属性
        write(prepare.getAttrs(), out);
        //校验码
        out.writeLong(prepare.getCrc());
        // 重写总长度
        int end = out.writerIndex();
        int size = end - begin;
        prepare.setSize(size);
        out.writerIndex(begin);
        out.writeInt(size);
        out.writerIndex(end);
        return out;
    }

    public static ByteBuf write(BrokerRollback rollback, ByteBuf out) throws Exception {
        /**
         * size+MAGIC+type
         */
        int begin = out.writerIndex();
        //长度占位
        out.writeInt(0);
        //魔术字符
        out.writeShort(BrokerMessage.MAGIC_LOG_CODE);
        //命令类型
        out.writeByte(rollback.getType());
        //日志位置
        out.writeLong(rollback.getJournalOffset());
        //存储时间
        out.writeLong(rollback.getStoreTime());
        //主题
        write(rollback.getTopic(), out);
        //事物ID
        write(rollback.getTxId(), out, SHORT_SIZE);
        write(rollback.getAttrs(), out);
        //校验码
        out.writeLong(rollback.getCrc());
        // 重写总长度
        int end = out.writerIndex();
        int size = end - begin;
        rollback.setSize(size);
        out.writerIndex(begin);
        out.writeInt(size);
        out.writerIndex(end);
        return out;
    }


    public static ByteBuf write(BrokerCommit commit, ByteBuf out) throws Exception {
        /**
         * size+MAGIC+type
         */
        int begin = out.writerIndex();
        //长度占位
        out.writeInt(0);
        //魔术字符
        out.writeShort(BrokerMessage.MAGIC_LOG_CODE);
        //命令类型
        out.writeByte(commit.getType());
        //日志位置
        out.writeLong(commit.getJournalOffset());
        //存储时间
        out.writeLong(commit.getStoreTime());
        //主题
        write(commit.getTopic(), out);
        //事物ID
        write(commit.getTxId(), out, SHORT_SIZE);
        //扩展属性
        write(commit.getAttrs(), out);
        //事物涉及的消息条数
        out.writeInt(commit.getMsgCount());
        //校验码
        out.writeLong(commit.getCrc());
        // 重写总长度
        int end = out.writerIndex();
        int size = end - begin;
        commit.setSize(size);
        out.writerIndex(begin);
        out.writeInt(size);
        out.writerIndex(end);
        return out;
    }

}

