package com.ipd.jmq.test.performance.safe;

import com.ipd.jmq.client.connection.ClusterTransportManager;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.client.consumer.MessageConsumer;
import com.ipd.jmq.client.consumer.MessageListener;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.model.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Created by lining11 on 2017/5/9.
 */
public class SafeConsumer {
    private static Logger log = LoggerFactory.getLogger(SafeConsumer.class);

    private ClusterTransportManager clusterTransportManager;

    private MessageConsumer messageConsumer;

    private static FileRecorder recorder = new FileRecorder();

    private String topic;

    private String app;

    private String address;

    private String password;

    private String user;

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public void init() {
        TransportConfig config = new TransportConfig();
        config.setAddress(address);
        config.setUser(user);
        config.setPassword(password);
        config.setApp(app);

        clusterTransportManager = new ClusterTransportManager(config);

        try {
            clusterTransportManager.start();
        } catch (Exception e) {
            log.error("", e);
        }

        ConsumerConfig consumerConfig = new ConsumerConfig();
        messageConsumer = new MessageConsumer(consumerConfig, clusterTransportManager, null);

        try {
            messageConsumer.start();
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public void subscribe(String topic) {

        messageConsumer.subscribe(topic, new DefaultMessageListener(topic).init());
    }

    public void setFileSize(int fileSize) {
        this.recorder.setFileSize(fileSize);
    }

    class DefaultMessageListener implements MessageListener {

        String bakPath = "/export/Data/jmq.safetest/bak";

        String topic = null;

        BufferedWriter writer = null;

        public DefaultMessageListener(String topic) {
            this.topic = topic;
        }

        public void onMessage(List<Message> messages) throws Exception {

            for (Message message : messages) {
                synchronized (recorder) {
                    recorder.addConsumeRecord(message.getBusinessId());
                    bakRecord(message.getBusinessId());
                }
            }
        }

        private void bakRecord(String businessId) {
            try {
                writer.write(businessId);
                writer.newLine();
                writer.flush();
            } catch (IOException e) {
                log.error("", e);
            }
        }

        public MessageListener init() {
            File path = new File(bakPath);
            if (!path.exists()) {
                path.mkdirs();
            }
            File bakFile = new File(path, "bak.txt");
            if (!bakFile.exists()) {
                try {
                    bakFile.createNewFile();
                } catch (IOException e) {
                    log.error("bak file error", e);
                }
            }
            FileWriter fileWriter = null;
            try {
                fileWriter = new FileWriter(bakFile);
            } catch (IOException e) {
                log.error("", e);
            }
            writer = new BufferedWriter(fileWriter);
            return this;
        }
    }
}
