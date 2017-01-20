package com.ipd.jmq.client.offset;

import com.ipd.jmq.client.consumer.offset.OffsetManage;
import com.ipd.jmq.client.consumer.offset.impl.FileOffsetManage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * 本地文件管理测试
 *
 * Created by zhangkepeng on 15-11-17.
 */
public class FileOffsetManageTest {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(FileOffsetManageTest.class);

    public OffsetManage offsetManage;
    public String fileOffsetPath = System.getProperty("user.home");
    public String clientName = "testFileOffsetManage";
    public String groupName = "testFileOffsetManage";
    public String topic = "testFileOffsetManage";
    public String app = "testFileOffsetManage";
    public short queueId = 1;

    @Before
    public void setUp() {
        try {
            offsetManage = FileOffsetManage.getInstance(fileOffsetPath);
            if (offsetManage instanceof FileOffsetManage) {
                offsetManage.start();
            }
        } catch (Exception e) {
            logger.info("file offset manage initialOrStart fail");
        }
    }

    @After
    public void shutDown() {
        if (offsetManage instanceof FileOffsetManage) {
            offsetManage.stop();
        }
    }

    /**
     *测试获取修改重置刷盘方法功能
     */
    @Test
    public void testFileOffsetManage() {
        long offset = -1;

        do {
            System.out.println("get offset is " + testGetOffset());
            offset++;
            System.out.println("reset offset is " + testResetOffset(offset));
            offset++;
            System.out.println("update offset is " + testUpdateOffset(offset));
            testFlushOnlyOne();
            System.out.println("finish flush!");
        } while (200 > offset);
    }

    public long testGetOffset() {
        long offset = offsetManage.getOffset(clientName, groupName, topic, queueId);
        return offset;
    }

    public long testResetOffset(long offsetValue) {
        long offset = offsetManage.resetOffset(clientName, groupName, topic, queueId, offsetValue, false);
        return offset;
    }

    public long testUpdateOffset(long offsetValue) {
        long offset = offsetManage.updateOffset(clientName, groupName, topic, queueId, offsetValue, false, false);
        return offset;
    }

    public void testFlushOnlyOne() {
        offsetManage.flush(topic, clientName);
    }

    /**
     * 统计具体应用某个分组当前偏移量代表的消息条数，不一定是消费数，消费数从订阅开始计算
     *
     * @param clientName
     * @param groupName
     * @param topic
     * @param queues
     * @return
     */
    public long testMessageCount(String clientName, String groupName, String topic, short queues) {
        long totalOffset = 0;
        long count;
        for (short queueId = 1; queueId <= queues; queueId++) {
            totalOffset += offsetManage.getOffset(clientName, groupName, topic, queueId);
        }
        count = totalOffset / 22;
        return count;
    }
}
