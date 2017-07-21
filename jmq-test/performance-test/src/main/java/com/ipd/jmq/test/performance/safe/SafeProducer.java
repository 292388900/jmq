package com.ipd.jmq.test.performance.safe;

import com.ipd.jmq.client.connection.ClusterTransportManager;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.client.producer.MessageProducer;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.model.Acknowledge;
import org.apache.log4j.Logger;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by lining11 on 2017/5/9.
 */
public class SafeProducer {

    private String key = "jmq:";

    private static final Logger logger = Logger.getLogger(SafeProducer.class);
    private TransportManager manager;
    private MessageProducer producer;
    private AtomicLong seri = new AtomicLong(0);
    private StringBuffer messageBuffer = new StringBuffer();
    private String sead = "1234567890!@#$%^&*()qwertyuioplkjhgfdsazxcvbnmQWERTYUIOPLKJHGFDSAZXCVBNM";
    private FileRecorder recorder = new FileRecorder();

    private String app;

    private String topic;

    private String address;

    private String user;

    private String password;

    private int ack = 1;

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public void init() throws Exception {
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
        config.setEpoll(false);

        //创建集群连接管理器
        manager = new ClusterTransportManager(config);
        manager.start();

        //创建发送者
        producer = new MessageProducer(manager);
        producer.getConfig().setAcknowledge(Acknowledge.valueOf(ack));
        logger.info(String.format("Ack:%s",Acknowledge.valueOf(ack).name()));
        producer.start();
        Random random = new Random();
        do {
            int size = random.nextInt();
            char c = sead.charAt(Math.abs(size) % sead.length());
            messageBuffer.append(c);
        } while (messageBuffer.length() < 1500);
    }

    public void testSend() {

        new Thread() {
            @Override
            public void run() {
                send();
            }
        }.start();
    }

    private void send() {
        while (true) {
            Message message = new Message();
            Random random = new Random();
            int head = random.nextInt(500);
            message.setText(messageBuffer.substring(head, messageBuffer.length() - 1 - head));
            message.setBusinessId(seri.getAndIncrement() + "");
            message.setTopic(topic);

            try {
                producer.send(message);
                recorder.addProduceRecord(seri.get() + "");
            } catch (JMQException e) {
                seri.decrementAndGet();
            }
        }
    }


    public void setFileSize(int fileSize) {
        this.recorder.setFileSize(fileSize);
    }

    public void setAck(int ack) {
        this.ack = ack;
    }
}
