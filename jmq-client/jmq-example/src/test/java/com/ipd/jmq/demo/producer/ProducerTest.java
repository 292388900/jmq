package com.ipd.jmq.demo.producer;

import com.ipd.jmq.client.producer.AsyncSendCallback;
import com.ipd.jmq.client.producer.MessageProducer;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.network.v3.command.PutMessage;
import com.ipd.jmq.common.network.v3.command.Command;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
* 发送测试类.
*/

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:spring/spring-producer.xml"})
public class ProducerTest {
    private static final Logger logger = Logger.getLogger(ProducerTest.class);
    @Resource(name = "producer")
    private MessageProducer producer;
    @Value("${jmq.producer.topic}")
    private String topic = "heng";

    @Test
    public void testMessageProducer() throws JMQException {
        for (int i = 0; i < 10000; i++) {
            try {
                send(i);
                if ((i + 1) % 100 == 0) {
                    System.out.println((i + 1) + " 条消息已发送");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testASyncSend() throws JMQException{
        //asyncSend(1);
        asyncBatchSend(1, 100);
    }

    /**
     * 非事务的单条发送
     */
    protected void send(int i) throws Exception {
        Message message = new Message(topic, "消息内容" + i, "业务ID" + i);
        producer.send(message);
       // logger.info("成功发送一条消息！！");
    }

    /**
     * 批量发送，不支持顺序消息，不支持事务
     */
    protected void batchSend(int i, int batchSize) throws JMQException {
        List<Message> messages = new ArrayList<Message>();
        for (int j = 0; j < batchSize; j++) {
            Message message = new Message(topic, "消息内容" + i, "业务ID" + i);
            messages.add(message);
        }

        producer.send(messages);
    }

    /**
     * 单条异步发送，不支持事务,
     */
    protected void asyncSend(int i) throws JMQException {
        Message message = new Message(topic, "消息内容" + i, "业务ID" + i);
        producer.sendAsync(message, new AsyncSendCallback() {
            @Override
            public void success(PutMessage putMessage, Command response) {
                System.out.println("async send success todo...");
            }

            @Override
            public void exception(PutMessage putMessage, Throwable cause) {
                System.out.println("async send failure todo...");
            }
        });
    }

    /**
     * 批量异步发送，不支持事务
     */
    protected void asyncBatchSend(int i, int batchSize) throws JMQException {
        List<Message> messages = new ArrayList<Message>();
        for (int j = 0; j < batchSize; j++) {
            Message message = new Message(topic, "消息内容" + i, "业务内容" + i);
            messages.add(message);
        }
        producer.sendAsync(messages, new AsyncSendCallback() {
            @Override
            public void success(PutMessage putMessage, Command response) {
                System.out.println("async send success todo...");
            }

            @Override
            public void exception(PutMessage putMessage, Throwable cause) {
                System.out.println("async send failure todo...");
            }
        });
    }

}
