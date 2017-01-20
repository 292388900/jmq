package com.ipd.jmq.common.message;

import com.ipd.jmq.common.model.JournalLog;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

/**
 * 存储的消息
 */
public class BrokerMessage extends Message implements Serializable, JournalLog {
    public static final short MAGIC_CODE = 0x1234;
    public static final short MAGIC_BLANK_CODE = 0x2345;
    public static final short MAGIC_LOG_CODE = 0x3456;//只是做个引用方便
    public static final short MESSAGE_FROM_KAFKA = 1;
    // 大小
    private int size;
    // 消息来源
    private short source;
    // 日志全局偏移量
    private long journalOffset;
    // 队列全局偏移量
    private long queueOffset;
    // 客户端地址
    private byte[] clientAddress;
    // 服务端地址
    private byte[] serverAddress;
    // 服务端收到时间
    private long receiveTime;
    // 服务端存储时间
    private long storeTime;
    // 重新
    private int redelivery;
    // 消息ID
    private MessageId messageId;
    // 事务ID
    private String txId;
    // 消息类型
    private byte type = JournalLog.TYPE_MESSAGE;


    @Override
    public BrokerMessage topic(final String topic) {
        setTopic(topic);
        return this;
    }

    @Override
    public BrokerMessage flag(final short flag) {
        setFlag(flag);
        return this;
    }

    @Override
    public BrokerMessage app(final String app) {
        setApp(app);
        return this;
    }

    @Override
    public BrokerMessage businessId(final String businessId) {
        setBusinessId(businessId);
        return this;
    }

    @Override
    public BrokerMessage priority(final byte priority) {
        setPriority(priority);
        return this;
    }

    @Override
    public BrokerMessage ordered(final boolean ordered) {
        setOrdered(ordered);
        return this;
    }

    @Override
    public BrokerMessage text(final String text) {
        setText(text);
        return this;
    }

    @Override
    public BrokerMessage attribute(final String key, final String value) {
        setAttribute(key, value);
        return this;
    }

    @Override
    public BrokerMessage attributes(final Map<String, String> attributes) {
        setAttributes(attributes);
        return this;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }


    public void setType(byte type) {
        this.type = type;
    }

    public BrokerMessage size(final int size) {
        setSize(size);
        return this;
    }

    public BrokerMessage source(final short source) {
        setSource(source);
        return this;
    }

    public BrokerMessage compressed(final boolean compressed) {
        setCompressed(compressed);
        return this;
    }

    public BrokerMessage journalOffset(final long journalOffset) {
        setJournalOffset(journalOffset);
        return this;
    }

    public BrokerMessage queueId(final short queueId) {
        setQueueId(queueId);
        return this;
    }

    public BrokerMessage queueOffset(final long queueOffset) {
        setQueueOffset(queueOffset);
        return this;
    }

    public BrokerMessage clientAddress(final byte[] clientAddress) {
        setClientAddress(clientAddress);
        return this;
    }

    public BrokerMessage serverAddress(final byte[] serverAddress) {
        setServerAddress(serverAddress);
        return this;
    }

//    public BrokerMessage sendTime(final long sendTime) {
//        setSendTime(sendTime);
//        return this;
//    }

    public BrokerMessage receiveTime(final long receiveTime) {
        setReceiveTime(receiveTime);
        return this;
    }

    public BrokerMessage storeTime(final long storeTime) {
        setStoreTime(storeTime);
        return this;
    }

    public BrokerMessage bodyCRC(final int bodyCRC) {
        setBodyCRC(bodyCRC);
        return this;
    }

    public BrokerMessage body(ByteBuffer body) {
        setBody(body);
        return this;
    }

    public BrokerMessage redelivery(final int redelivery) {
        setRedelivery(redelivery);
        return this;
    }

    public short getSource() {
        return source;
    }

    public void setSource(short source) {
        this.source = source;
    }

    public int getSize() {
        return this.size;
    }

    @Override
    public byte getType() {
        return type;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public long getJournalOffset() {
        return this.journalOffset;
    }

    public void setJournalOffset(long journalOffset) {
        this.journalOffset = journalOffset;
    }

    public long getQueueOffset() {
        return this.queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public byte[] getClientAddress() {
        return this.clientAddress;
    }

    public void setClientAddress(byte[] clientAddress) {
        this.clientAddress = clientAddress;
    }

    public byte[] getServerAddress() {
        return this.serverAddress;
    }

    public void setServerAddress(byte[] serverAddress) {
        this.serverAddress = serverAddress;
    }

    public long getReceiveTime() {
        return this.receiveTime;
    }

    public void setReceiveTime(long receiveTime) {
        this.receiveTime = receiveTime;
    }

    public long getStoreTime() {
        return this.storeTime;
    }

    public void setStoreTime(long storeTime) {
        this.storeTime = storeTime;
    }

    @Override
    public String getEndPoint() {
        return topic;
    }

    public int getRedelivery() {
        return this.redelivery;
    }

    public void setRedelivery(int redelivery) {
        this.redelivery = redelivery;
    }

    public MessageId getMessageId() {
        if (messageId == null) {
            messageId = new MessageId(serverAddress, journalOffset);
        }
        return messageId;
    }

    public void setMessageId(MessageId messageId) {
        this.messageId = messageId;
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

        BrokerMessage message = (BrokerMessage) o;

        if (journalOffset != message.journalOffset) {
            return false;
        }
        if (queueId != message.queueId) {
            return false;
        }
        if (queueOffset != message.queueOffset) {
            return false;
        }
        if (receiveTime != message.receiveTime) {
            return false;
        }
        if (redelivery != message.redelivery) {
            return false;
        }
        if (sendTime != message.sendTime) {
            return false;
        }
        if (size != message.size) {
            return false;
        }
        if (source != message.source) {
            return false;
        }
        if (storeTime != message.storeTime) {
            return false;
        }
        if (!Arrays.equals(clientAddress, message.clientAddress)) {
            return false;
        }
        if (messageId != null ? !messageId.equals(message.messageId) : message.messageId != null) {
            return false;
        }
        if (!Arrays.equals(serverAddress, message.serverAddress)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + size;
        result = 31 * result + source;
        result = 31 * result + (int) (journalOffset ^ (journalOffset >>> 32));
        result = 31 * result + (int) queueId;
        result = 31 * result + (int) (queueOffset ^ (queueOffset >>> 32));
        result = 31 * result + (clientAddress != null ? Arrays.hashCode(clientAddress) : 0);
        result = 31 * result + (serverAddress != null ? Arrays.hashCode(serverAddress) : 0);
        result = 31 * result + (int) (sendTime ^ (sendTime >>> 32));
        result = 31 * result + (int) (receiveTime ^ (receiveTime >>> 32));
        result = 31 * result + (int) (storeTime ^ (storeTime >>> 32));
        result = 31 * result + redelivery;
        result = 31 * result + (messageId != null ? messageId.hashCode() : 0);
        return result;
    }
}