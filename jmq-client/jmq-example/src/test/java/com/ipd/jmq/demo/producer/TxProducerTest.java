package com.ipd.jmq.demo.producer;

import com.ipd.jmq.client.producer.MessageProducer;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.network.v3.command.TxStatus;
import com.ipd.jmq.toolkit.time.SystemClock;
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
 * TxProducerTest
 *
 * @author luoruiheng
 * @since 8/8/16
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:spring/spring-producer-tx.xml"})
public class TxProducerTest {

    private static final Logger logger = Logger.getLogger(TxProducerTest.class);

    @Resource(name = "producer")
    private MessageProducer producer;
    @Value("${jmq.producer.topic}")
    private String topic;

    @Test
    public void testSendTxMessage() {
        int batchSize = 100;
        int txTimeout = 5000;
        String queryId = topic + "_queryId" + SystemClock.now();
        String txId = null;

        List<Message> messages = new ArrayList<Message>();
        for (int i = 0; i < batchSize; i++) {
            Message message = new Message(topic, "msg: " + i, "bizID: " + i);
            messages.add(message);
        }

        try {

            // todo 本地事务 PREPARE
            // your code

            // JMQ事务 PREPARE
            txId = producer.beginTransaction(topic, queryId, txTimeout);

            // todo 本地事务业务
            // your code

            // JMQ事务 SEND
            producer.send(messages);

            // 保存本地事务状态为 COMMIT
            TxStatusUtil.set(txId, TxStatus.COMMITTED.ordinal() + "");

            /**
             * todo 本地事务 COMMIT, 根据实际情况修改
             *
             * 使用方应保证自己的本地事务 COMMIT 成功
             *
             */
            // your code

            try {
                // JMQ事务 COMMIT
                producer.commitTransaction();
                // logger.info("成功提交 {} 条消息!", batchSize);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                // JMQ事务如果 COMMIT 失败，此时交由 FEEDBACK 机制处理
                // (因为本地事务状态已成功保存为 COMMITTED)
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            // 本地事务 COMMIT 之前抛出异常，都尝试 ROLLBACK
            rollback(txId, queryId);
        }
    }

    @Test
    public void testSendTxMsgWithoutLocalTx() {
        int txTimeout = 5000;
        String queryId = topic + "_queryId" + SystemClock.now();

        List<Message> messages = new ArrayList<Message>();
        for (int i = 0; i < 20; i++) {
            Message message = new Message(topic, "msgTx: " + i, "bizID: " + i);
            /*Message message = new Message(topic,
                    "msgTx_shfgjklafjglkjgsklfjglkjfsklgjoifgjlkafjgklajgkljajgklajfgkljaiogjrileajhglkajhlkjakljhoiajhoiajhiajhlkajklgajklhgjaklhjaklhjlkahjklahjkalhjklajhklsdfjhoiadfjhoiasfjhoithfahafdhaa" + i,
                    "bzID_" + i);*/
            messages.add(message);

            if (messages.size() % 10 == 0) {
                sendWithoutLocalTx(messages, topic, queryId, txTimeout);
                messages.clear();
            }
        }

    }

    /**
     * 没有本地事务
     *
     * @param messages  消息列表
     * @param topic     主题
     * @param queryId   查询 ID
     * @param txTimeout 事务超时时间
     */
    private void sendWithoutLocalTx(List<Message> messages, String topic, String queryId, int txTimeout) {
        String txId = null;
        try {
            txId = producer.beginTransaction(topic, queryId, txTimeout);
            producer.send(messages);
            producer.commitTransaction();
            logger.info(messages.size() + " message(s) committed successfully.");
        } catch (JMQException e) {
            if (null != txId) {
                try {
                    producer.rollbackTransaction();
                } catch (Exception e1) {
                    logger.error(e1.getMessage(), e1);
                }
            }
        }
    }

    private void rollback(String txId, String queryId) {
        // ROLLBACK
        if (null != txId && !txId.isEmpty()) {
            try {
                // 保存事务状态为 ROLLBACK
                TxStatusUtil.set(txId, TxStatus.ROLLBACK.ordinal() + "");
            } catch (Exception e) {
                logger.warn("保存事务状态为ROLLBACK时出错!尝试再次保存...");
                // 再次保存
                try {
                    TxStatusUtil.set(txId, TxStatus.ROLLBACK.ordinal() + "");
                } catch (Exception e1) {
                    e1.printStackTrace();
                    logger.error("两次保存事务状态为ROLLBACK时出错!");
                }
            } finally {
                /**
                 * todo 本地事务 ROLLBACK，根据实际情况修改
                 *
                 * 使用方应保证自己的本地事务 ROLLBACK 成功
                 */
                // your code

                try {
                    producer.rollbackTransaction();
                } catch (JMQException e) {
                    // JMQ事务 ROLLBACK 出错，交由 FEEDBACK 处理
                    // 即使事务状态 ROLLBACK 没有保存成功，服务端超时后也会 ROLLBACK
                    e.printStackTrace();
                }
            }
        }
    }

}
