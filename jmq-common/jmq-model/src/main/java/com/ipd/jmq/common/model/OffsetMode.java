package com.ipd.jmq.common.model;

/**
 * 偏移量管理类型,分为本地与服务端
 *
 * Created by zhangkepeng on 15-10-23.
 */
public enum OffsetMode {
    REMOTE,
    LOCAL;

    public static OffsetMode valueOf(int ordinal) {
        if (ordinal < 0 || ordinal >= values().length) {
            throw new IndexOutOfBoundsException("Invalid ordinal");
        }
        return values()[ordinal];
    }
}
