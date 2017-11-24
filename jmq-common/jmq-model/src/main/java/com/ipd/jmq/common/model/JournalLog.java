package com.ipd.jmq.common.model;


/**
 * Created by dingjun on 16-5-5.
 */
public interface JournalLog {
    //日志类型
    public static final byte TYPE_TX_PRE_MESSAGE=(byte)1;
    public static final byte TYPE_TX_MESSAGE=(byte)2;
    public static final byte TYPE_TX_PREPARE=(byte)3;
    public static final byte TYPE_TX_COMMIT=(byte)4;
    public static final byte TYPE_TX_ROLLBACK=(byte)5;
    public static final byte TYPE_MESSAGE=(byte)6;
    public static final byte TYPE_REF_MESSAGE=(byte)7;

    public int getSize();
    public byte getType();
    public long getJournalOffset();
    public long getStoreTime();
    public void setStoreTime(long storeTime);
    public String getEndPoint();

}
