package com.ipd.jmq.client.consumer;

import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.client.consumer.offset.OffsetManage;
import com.ipd.jmq.client.consumer.offset.impl.FileOffsetManage;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.MessageLocation;
import com.ipd.jmq.common.model.ConsumerConfig;
import com.ipd.jmq.toolkit.concurrent.NamedThreadFactory;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 消费者容器
 */
public class MessageConsumer extends Service implements Consumer, PullConsumer {
    private static final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);
    //传输通道管理器
    protected TransportManager transportManager;
    //消费者配置
    protected ConsumerConfig config;
    //主题消费者集合
    protected ConcurrentMap<String, TopicConsumer> consumers = new ConcurrentHashMap<String, TopicConsumer>();
    //任务调度器
    protected ScheduledExecutorService schedule;
    //监听器配置
    private Set<TopicSetting> settings;
    // 本地offset管理
    private OffsetManage localOffsetManage;
    // 广播客户端实例名称
    protected String clientName;

    public MessageConsumer() {
        this(new ConsumerConfig(), null, null);
    }

    public MessageConsumer(ConsumerConfig config, TransportManager transportManager, Set<TopicSetting> settings) {
        this.config = config;
        this.transportManager = transportManager;
        this.settings = settings;
    }

    @Override
    protected void validate() throws Exception {
        super.validate();
        if (transportManager == null) {
            throw new IllegalStateException("transportManager can not be null");
        }
        if (config == null) {
            throw new IllegalStateException("config can not be null");
        }
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        schedule = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("ConsumerGuarder"));
        if (!transportManager.isStarted()) {
            transportManager.start();
        }
        if (clientName == null) {
            clientName = config.getClientName();
        }
        if (localOffsetManage == null) {
            localOffsetManage = FileOffsetManage.getInstance(transportManager.getConfig().getTempPath());
            ((FileOffsetManage) localOffsetManage).setPersistConsumerOffsetInterval(config.getPersistConsumerOffsetInterval());
        }
        if (settings != null && !settings.isEmpty()) {
            for (TopicSetting setting : settings) {
                ConsumerConfig cfg = setting.getConfig() != null ? setting.getConfig() : this.config;
                subscribe(setting.getTopic(), setting.getListener(), cfg);
            }
        }
        logger.info("message consumer is started");
    }

    @Override
    protected void doStop() {
        for (TopicConsumer consumer : consumers.values()) {
            consumer.stop();
        }
        Close.close(schedule);
        super.doStop();
        logger.info("message consumer is stopped");
    }

    @Override
    public void pause() {
        if (config != null) {
            config.setAutoStart(false);
        }
        for (TopicConsumer consumer : consumers.values()) {
            consumer.pause();
        }
    }

    @Override
    public void pause(String topic) {
        TopicConsumer consumer = consumers.get(topic);
        if (consumer != null) {
            consumer.pause();
        }
    }

    @Override
    public boolean isPaused(String topic) {
        TopicConsumer consumer = consumers.get(topic);
        if (consumer != null) {
            return consumer.isPaused();
        }

        return false;
    }

    @Override
    public void resume() {
        if (config != null) {
            config.setAutoStart(true);
        }
        for (TopicConsumer consumer : consumers.values()) {
            consumer.resume();
        }
    }

    @Override
    public void resume(String topic) {
        TopicConsumer consumer = consumers.get(topic);
        if (consumer != null) {
            consumer.resume();
        }
    }

    @Override
    public void subscribe(String topic, MessageListener listener) {
        subscribe(topic, listener, null);
    }

    @Override
    public void subscribe(String topic, MessageListener listener, ConsumerConfig config) {
        if (!isStarted()) {
            throw new IllegalStateException("consumer is not started");
        }

        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic can not be empty");
        }
        if (listener == null) {
            throw new IllegalArgumentException("listener can not be null");
        }

        TopicConsumer consumer = getConsumer(topic, null, listener, ConsumerType.LISTENER, config);
        if (consumer.getType() == ConsumerType.PULL) {
            throw new IllegalStateException(String.format("%s is in pull model.", topic));
        }
    }

    @Override
    public void unSubscribe(String topic) {
        TopicConsumer consumer = consumers.remove(topic);
        if (consumer != null) {
            Close.close(consumer);
        }
    }

    public void setSettings(Set<TopicSetting> settings) {
        this.settings = settings;
    }

    /**
     * 获取消费者
     *
     * @param topic    主题
     * @param selector 选择器
     * @param listener 监听器
     * @param type     类型
     * @return 主题消费者对象
     */
    protected TopicConsumer getConsumer(String topic, String selector, MessageListener listener, ConsumerType type,
                                        ConsumerConfig config) {
        String app = transportManager.getConfig().getApp();
        topic = topic.trim();
        if (topic.isEmpty()) {
            throw new IllegalArgumentException("topic can not be empty");
        }

        config = config != null ? config : this.config;
        TopicConsumer consumer = consumers.get(topic);
        if (consumer == null) {
            consumer = new TopicConsumer(topic, selector, listener);

            consumer.setConfig(config);
            consumer.setTransportManager(transportManager);
            consumer.setSchedule(schedule);
            consumer.setType(type);
            consumer.setLocalOffsetManage(localOffsetManage);
            consumer.setApp(app);
            consumer.setClientName(clientName);
            // 添加到消费者集合里面
            TopicConsumer old = consumers.putIfAbsent(topic, consumer);
            if (old == null) {
                try {
                    // 不会抛出异常
                    if (!config.isAutoStart()) {
                        logger.warn("autoStart is false, consumer is paused!! you can call resume() to start consume!");
                        consumer.pause();
                    }
                    consumer.start();
                } catch (Exception ignored) {
                    logger.error("", ignored);
                }
            }
        }
        return consumer;
    }

    @Override
    public int pull(String topic, MessageListener listener) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic can not be empty");
        }
        if (listener == null) {
            throw new IllegalArgumentException("listener can not be null");
        }
        TopicConsumer consumer = getConsumer(topic, null, null, ConsumerType.PULL, config);
        if (consumer.getType() == ConsumerType.LISTENER) {
            throw new IllegalStateException(String.format("%s is in listener model.", topic));
        }
        return consumer.pull(listener);
    }



    // 从缓存中取消息
    @Override
    public PullResult pull(String topic) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic can not be empty");
        }
        TopicConsumer consumer = getConsumer(topic, null, null, ConsumerType.PULL, config);
        if (consumer.getType() == ConsumerType.LISTENER) {
            throw new IllegalStateException(String.format("%s is in listener model.", topic));
        }

        return new PullResultHandler(consumer);
    }

    /**
     * 应答消息，目前消息来源于同一个Broker下的同一个消费者
     *
     * @param topic       主题
     * @param consumerId  消费Id
     * @param brokerGroup jmq group
     * @param locations   位置列表
     * @throws com.ipd.jmq.common.exception.JMQException
     */
    public void acknowledge(String topic, String consumerId, String brokerGroup, MessageLocation[] locations) throws
            JMQException {
        TopicConsumer consumer = consumers.get(topic);
        if (consumer != null) {
            consumer.acknowledge(consumerId, brokerGroup, locations, true);
        } else {
            throw new JMQException("consumer is null,consumerId:" + consumerId + ",group:" + brokerGroup + ",topic:" + topic, JMQCode.CN_CONNECTION_ERROR.getCode());
        }
    }

    /**
     * * 重试消息，目前只包含一组的数据
     *
     * @param topic       主题
     * @param consumerId  消费Id
     * @param brokerGroup jmq group
     * @param locations   位置列表
     * @param exception   异常信息
     * @throws JMQException
     */
    public void retry(String topic, String consumerId, String brokerGroup, MessageLocation[] locations, String exception) throws JMQException {
        TopicConsumer consumer = consumers.get(topic);
        if (consumer != null) {
            consumer.retry(consumerId, brokerGroup, locations, exception);
        } else {
            throw new JMQException("consumer is null,consumerId:" + consumerId + ",group:" + brokerGroup + ",topic:" + topic, JMQCode.CN_CONNECTION_ERROR.getCode());
        }
    }


    public void flush(String topic) {
        TopicConsumer consumer = consumers.get(topic);
        if (consumer != null) {
            consumer.flush();
        }
    }

    public OffsetManage getLocalOffsetManage() {
        return localOffsetManage;
    }

    public void setLocalOffsetManage(OffsetManage localOffsetManage) {
        this.localOffsetManage = localOffsetManage;
    }

    public TransportManager getTransportManager() {
        return transportManager;
    }

    public String getClientName() {
        return clientName;
    }

    public ConsumerConfig getConfig() {
        return config;
    }
}