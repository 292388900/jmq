package com.ipd.jmq.server.store;

import java.io.IOException;

/**
 * Created by guoliang5 on 2016/9/5.
 */
public interface StoreUnsafe {
    void updateWaterMark(long waterMark);
    void truncate(long offset) throws IOException;
}
