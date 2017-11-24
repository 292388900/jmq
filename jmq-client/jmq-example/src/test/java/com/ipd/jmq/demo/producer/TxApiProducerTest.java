package com.ipd.jmq.demo.producer;

import com.ipd.jmq.client.connection.ClusterTransportManager;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.client.producer.MessageProducer;
import com.ipd.jmq.client.transaction.TxFeedbackManager;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.network.v3.command.TxStatus;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 生产测试
 */
public class TxApiProducerTest {
    private static final Logger logger = LoggerFactory.getLogger(TxApiProducerTest.class);
    private TransportManager manager;
    private MessageProducer producer;
    // @Value("${jmq.producer.topic}")
    private String topic = "heng";
    // @Value("${jmq.producer.app}")
    private String app = "hengApp";
    //  @Value("${jmq.address}")
    private String address = "10.42.0.1:50088";

    @Before
    public void setUp() throws Exception {
        //连接配置
        TransportConfig config = new TransportConfig();
        config.setApp(app);
        //设置broker地址
        config.setAddress(address);
        //设置用户名
        config.setUser("jmq");
        //设置密码
        config.setPassword("jmq");
        //设置发送超时
        config.setSendTimeout(10000);
        //设置是否使用epoll模式，windows环境下设置为false，linux环境下设置为true
        config.setEpoll(true);

        //创建集群连接管理器
        manager = new ClusterTransportManager(config);
        manager.start();

        //创建发送者
        producer = new MessageProducer(manager);
        
        // TxFeedbackManager
        TxFeedbackManager txFeedbackManager = new TxFeedbackManager(manager);
        txFeedbackManager.addTxStatusQuerier(topic, new LocalTxStatusQuerier());
        producer.setTxFeedbackManager(txFeedbackManager);
        
        producer.start();

    }

    @After
    public void terDown() throws Exception {
        if (producer != null) {
            producer.stop();
        }
        if (manager != null) {
            manager.stop();
        }
    }

    /**
     * 事务消息发送测试
     */
    @Test
    public void commitTest() {
        int batchSize = 10;
        int txTimeout = 5000;
        String queryId = topic + "_queryId_" + SystemClock.now();

        List<Message> messages = new ArrayList<Message>();
        for (int i = 0; i < batchSize; i++) {
            Message message = new Message(topic, "msg: " + i, "bizID: " + i);
            messages.add(message);
        }

        for (int i = 0; ; i++) {
            batchSendAndCommitTxMsg(queryId, txTimeout, messages);
        }
    }

    @Test
    public void rollbackTest() {
        int batchSize = 10;
        int txTimeout = 5000;
        String queryId = topic + "_queryId_" + SystemClock.now();

        List<Message> messages = new ArrayList<Message>();
        for (int i = 0; i < batchSize; i++) {
            Message message = new Message(topic, "msg: " + i, "bizID: " + i);
            messages.add(message);
        }

        batchSendAndRollbackTxMsg(queryId, txTimeout, messages);
    }

    public void batchSendAndRollbackTxMsg(String queryId, int txTimeout, List<Message> messages) {
        String txId = null;
        try {

            // todo 本地事务 PREPARE
            // your code

            // JMQ事务 PREPARE
            txId = producer.beginTransaction(topic, queryId, txTimeout);

            // todo 本地事务业务
            // your code

            // JMQ事务 SEND
            producer.send(messages);

            // something went wrong..
            throw new Exception("testing rollback...");

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            // 本地事务 COMMIT 之前抛出异常，都尝试 ROLLBACK
            rollback(txId, queryId);
        }
    }

    public void batchSendAndCommitTxMsg(String queryId, int txTimeout, List<Message> messages) {
        String txId = null;
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
                logger.info("成功提交 {} 条消息!", messages.size());
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
    
    private void rollback(String txId, String queryId) {
        // ROLLBACK
        if (null != txId && !txId.isEmpty()) {
            try {
                // 保存事务状态为 ROLLBACK
                TxStatusUtil.set(txId, TxStatus.ROLLBACK.ordinal() + "");
            } catch (Exception e) {
                logger.warn("保存事务状态为 ROLLBACK 时出错! 尝试再次保存...");
                // 再次保存
                try {
                    TxStatusUtil.set(txId, TxStatus.ROLLBACK.ordinal() + "");
                } catch (Exception e1) {
                    logger.error("两次保存事务状态为 ROLLBACK 时出错!", e1);
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
                } catch (Exception e) {
                    // JMQ事务 ROLLBACK 出错，交由 FEEDBACK 处理
                    // 即使事务状态 ROLLBACK 没有保存成功，服务端超时后也会 ROLLBACK
                    logger.error(e.getMessage(), e);
                }

            }
        }
    }
    
}
