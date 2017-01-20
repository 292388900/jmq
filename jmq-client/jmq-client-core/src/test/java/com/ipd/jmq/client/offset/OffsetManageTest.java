package com.ipd.jmq.client.offset;

import com.ipd.jmq.client.consumer.offset.impl.FileOffsetManage;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by zhangkepeng on 15-9-18.
 */
public class OffsetManageTest {

    private String rootPath = System.getProperty("user.home");
    private FileOffsetManage fileOffsetManage = new FileOffsetManage(rootPath);

    @Test
    public void testOffsetManage() {
        long mRet;
        long offsetId1 = 8;
        long offsetId2 = 10;
        mRet = fileOffsetManage.resetOffset("localoffset", "jmq1", "offsetTest", (short) 2, offsetId1, true);
        assert (mRet == offsetId1);
        mRet = fileOffsetManage.updateOffset("localoffset", "jmq1", "offsetTest", (short) 2, offsetId2, true, true);
        assert (mRet == offsetId2);
        mRet = fileOffsetManage.getOffset("localoffset", "jmq1", "offsetTest", (short) 2);
        assert (mRet == offsetId2);

    }

}
