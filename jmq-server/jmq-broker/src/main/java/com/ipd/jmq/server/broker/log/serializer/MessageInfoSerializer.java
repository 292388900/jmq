package com.ipd.jmq.server.broker.log.serializer;


import com.ipd.jmq.common.model.ArchiveMessage;
import com.ipd.jmq.common.model.MessageInfo;
import com.ipd.jmq.toolkit.URL;
import com.ipd.jmq.toolkit.time.SystemClock;

import java.nio.ByteBuffer;

/**
 * @author hexiaofeng
 */
public class MessageInfoSerializer implements LogSerializer {

    @Override
    public ByteBuffer encode(final Object item, final ByteBuffer buffer) throws Exception {
        if (item == null || buffer == null || !(item instanceof MessageInfo)) {
            return null;
        }
        // 记录起始位置
        int start = buffer.position();
        // buffer.clear();
        // 写入长度(占位)
        buffer.putInt(0);

        MessageInfo info = (MessageInfo) item;
        String businessId = info.getBusinessId();
        Serializer.writeString(buffer, info.getMessageId());
        Serializer.writeString(buffer, info.getTopic());
        Serializer.writeString(buffer, info.getApp());
        if (businessId != null && businessId.length() > 16) {
            businessId = businessId.substring(0, 16);
        }
        Serializer.writeString(buffer, businessId);
        Serializer.writeString(buffer, info.getClientIp());
        buffer.putLong(info.getJournalOffset());
        buffer.putInt(info.getJournalSize());
        buffer.putLong(info.getSendTime());
        buffer.putLong(info.getReceiveTime());
        // 记录当前位置
        int end = buffer.position();
        // 在头部写入长度
        buffer.position(start);
        buffer.putInt(end - start - 4);
        // 恢复位置
        buffer.position(end);
        // buffer.flip();
        return buffer;
    }

    @Override
    public ArchiveMessage decode(final ByteBuffer buffer, final long offset) throws Exception {
        if (buffer == null) {
            return null;
        }
        MessageInfo info = new MessageInfo();
        info.setMessageId(Serializer.readString(buffer));
        info.setTopic(Serializer.readString(buffer));
        info.setApp(Serializer.readString(buffer));
        info.setBusinessId(Serializer.readString(buffer));
        info.setClientIp(Serializer.readString(buffer));
        info.setJournalOffset(buffer.getLong());
        info.setJournalSize(buffer.getInt());
        info.setSendTime(buffer.getLong());
        info.setReceiveTime(buffer.getLong());
        info.setCreateTime(SystemClock.getInstance().now());
        info.setOffset((int) offset);
        return info;
    }

    @Override
    public String getType() {
        return "JMQ.IN";
    }

    @Override
    public void setUrl(URL url) {

    }
}
