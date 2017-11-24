package com.ipd.jmq.demo.consumer;

import com.ipd.jmq.client.consumer.MessageConsumer;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.concurrent.CountDownLatch;

/**
 * 消费测试类
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:spring/spring-consumer.xml"})
public class ConsumerTest {
    private static final Logger logger = Logger.getLogger(ConsumerTest.class);

    @Resource(name = "consumer")
    private MessageConsumer consumer;

    @Test
    public void testConsume() throws Exception {
        try {
            logger.info("开始监听...");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        new CountDownLatch(1).await();
    }
}
