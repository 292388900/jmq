package com.ipd.jmq.server.broker.log.serializer;


import com.ipd.jmq.common.model.ArchiveMessage;
import com.ipd.jmq.common.model.ConsumeHistory;
import com.ipd.jmq.toolkit.URL;
import com.ipd.jmq.toolkit.time.SystemClock;

import java.nio.ByteBuffer;

/**
 * @author hexiaofeng
 */
public class ConsumeHistorySerializer implements LogSerializer {

    @Override
    public ByteBuffer encode(final Object item, final ByteBuffer buffer) throws Exception {
        if (item == null || buffer == null || !(item instanceof ConsumeHistory)) {
            return null;
        }
        // 记录起始位置
        int start = buffer.position();
        // buffer.clear();
        // 写入长度(占位)
        buffer.putInt(0);

        ConsumeHistory info = (ConsumeHistory) item;
        Serializer.writeString(buffer, info.getMessageId());
        Serializer.writeString(buffer, info.getTopic());
        buffer.putLong(info.getConsumeTime());
        Serializer.writeString(buffer, info.getClientIp());
        Serializer.writeString(buffer, info.getApp());
        buffer.putLong(info.getCreateTime());

        int end = buffer.position();
        buffer.position(start);
        buffer.putInt(end - start - 4);
        buffer.position(end);
        // buffer.flip();

        return buffer;
    }

    @Override
    public ArchiveMessage decode(final ByteBuffer buffer, final long offset) throws Exception {
        if (buffer == null) {
            return null;
        }
        ConsumeHistory info = new ConsumeHistory();

        info.setMessageId(Serializer.readString(buffer));
        info.setTopic(Serializer.readString(buffer));
        info.setConsumeTime(buffer.getLong());
        info.setClientIp(Serializer.readString(buffer));
        info.setApp(Serializer.readString(buffer));
        info.setCreateTime(SystemClock.getInstance().now());

        return info;

    }

    @Override
    public String getType() {
        return "JMQ.OUT";
    }

    @Override
    public void setUrl(URL url) {

    }
}
