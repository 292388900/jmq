package com.ipd.jmq.client.consumer.offset;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhangkepeng on 15-8-24.
 */
public class LocalOffsetSerializeWrapper extends DefaultSerializable{
    private ConcurrentHashMap<LocalMessageQueue, AtomicLong> offsetTable =
            new ConcurrentHashMap<LocalMessageQueue, AtomicLong>();

    public ConcurrentHashMap<LocalMessageQueue, AtomicLong> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(ConcurrentHashMap<LocalMessageQueue, AtomicLong> offsetTable) {
        this.offsetTable = offsetTable;
    }
}
