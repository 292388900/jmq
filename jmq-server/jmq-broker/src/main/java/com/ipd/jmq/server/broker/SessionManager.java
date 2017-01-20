package com.ipd.jmq.server.broker;

import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.network.v3.session.Connection;
import com.ipd.jmq.common.network.v3.session.Consumer;
import com.ipd.jmq.common.network.v3.session.Producer;
import com.ipd.jmq.server.broker.cluster.SequentialManager;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.toolkit.concurrent.EventBus;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 会话管理
 * User: weiqisong
 * Date: 14-4-23
 * Time: 上午10:54
 * 该类负责连接，生产者，消费者信息管理。
 */
public class SessionManager extends Service {
    private static final Logger logger = LoggerFactory.getLogger(SessionManager.class);
    // 绑定的连接Key
    public static final String CONNECTION_KEY = "connection";
    // 绑定的用户Key
    public static final String USER_KEY = "user";

    // 保存连接信息
    private ConcurrentMap<String, Connection> connections = new ConcurrentHashMap<String, Connection>();
    // 保存生产者信息
    private ConcurrentMap<String, Producer> producers = new ConcurrentHashMap<String, Producer>();
    // 保存消费者信息
    private ConcurrentMap<String, Consumer> consumers = new ConcurrentHashMap<String, Consumer>();
    // 事件管理器
    private EventBus<SessionEvent> eventManager = new EventBus<SessionEvent>();
    // 事件监听器
    protected EventListener<SessionEvent> sessionListener;
    //
    protected Thread sinkProducerThread;

    // 顺序消息管理器
    protected SequentialManager sequentialManager;

    protected ConcurrentMap<String, ConcurrentMap<String, Object>> consumerIds = new ConcurrentHashMap<String, ConcurrentMap<String, Object>>();
    private static final Object PRESENT = new Object();


    @Override
    protected void doStart() throws Exception {
        super.doStart();
        eventManager.start();

        this.sessionListener = new EventListener<SessionEvent>() {
            @Override
            public void onEvent(SessionManager.SessionEvent event) {
                if (isStarted()) {
                    try {
                        switch (event.getType()) {
                            case RemoveProducer:
                                onRemoveProducer(event.getProducer());
                                break;
                        }
                    } catch (JMQException e) {
                        if (isStarted()) {
                            logger.error(e.getMessage());
                        }
                    }
                }
            }
        };
        eventManager.addListener(sessionListener);
        logger.info("session manager is started");
    }

    @Override
    protected void doStop() {
        super.doStop();
        eventManager.stop();
        if (sinkProducerThread != null) {
            sinkProducerThread.interrupt();
        }
        sinkProducerThread = null;
        logger.info("session manager is stopped");
    }

    /**
     * 添加链接
     *
     * @param connection 连接
     * @return <li>true 成功</li>
     * <li>false 已经存在</li>
     */
    public boolean addConnection(final Connection connection) {
        if (connection == null) {
            return false;
        }
        if (connections.putIfAbsent(connection.getId(), connection) == null) {
            eventManager.add(new SessionEvent(SessionEventType.AddConnection, connection));
            if (logger.isDebugEnabled()) {
                logger.debug("add connection :" + connection.getId());
            }
            return true;
        }
        return false;
    }

    /**
     * 删除链接
     *
     * @param connectionId 连接ID
     */
    public void removeConnection(final String connectionId) {
        Connection connection = connections.remove(connectionId);
        if (connection == null) {
            return;
        }
        removeConsumer(connection);
        removeProducer(connection);

        eventManager.add(new SessionEvent(SessionEventType.RemoveConnection, connection));

    }

    /**
     * 根据连接删除生产者
     *
     * @param connection 连接
     */
    protected void removeProducer(final Connection connection) {
        if (connection == null) {
            return;
        }
        String connectionId = connection.getId();
        Producer producer;
        Map.Entry<String, Producer> entry;
        Iterator<Map.Entry<String, Producer>> it = producers.entrySet().iterator();
        while (it.hasNext()) {
            entry = it.next();
            producer = entry.getValue();
            if (producer.getConnectionId().equals(connectionId)) {
                it.remove();
                eventManager.add(new SessionEvent(SessionEventType.RemoveProducer, producer));
            }
        }
    }

    public void setSequentialManager(SequentialManager sequentialManager) {
        this.sequentialManager = sequentialManager;
    }

    /**
     * 根据连接删除消费者
     *
     * @param connection
     */
    protected void removeConsumer(final Connection connection) {
        if (connection == null) {
            return;
        }
        String connectionId = connection.getId();
        // 删除消费者
        Consumer consumer;
        Map.Entry<String, Consumer> entry;
        Iterator<Map.Entry<String, Consumer>> it = consumers.entrySet().iterator();
        while (it.hasNext()) {
            entry = it.next();
            consumer = entry.getValue();
            if (consumer.getConnectionId().equals(connectionId)) {
                it.remove();
                removeConsumerId(consumer);
                eventManager.add(new SessionEvent(SessionEventType.RemoveConsumer, consumer));
            }
        }
    }

    private void removeConsumerId(Consumer consumer) {
        ConcurrentMap<String, Object> idMap = consumerIds.get(getConsumerIdKey(consumer.getApp(), consumer.getTopic()));
        if (idMap != null) {
            idMap.remove(consumer.getId());
        }
    }

    private void addConsumerId(Consumer consumer) {
        ConcurrentMap<String, Object> idMap = consumerIds.get(getConsumerIdKey(consumer.getApp(), consumer.getTopic()));
        if (idMap == null) {
            idMap = new ConcurrentHashMap<String, Object>();
            ConcurrentMap<String, Object> old = consumerIds.putIfAbsent(getConsumerIdKey(consumer.getApp(), consumer.getTopic()), idMap);
            if (old != null) {
                idMap = old;
            }
        }
        idMap.putIfAbsent(consumer.getId(), PRESENT);
    }

    /**
     * 有效的消费者数
     *
     * @param app
     * @param topic
     * @param ackTimeout
     * @return
     */
    public int getConsumerSize(String app, String topic, long ackTimeout) {
        int size = 0;
        ConcurrentMap<String, Object> idMap = consumerIds.get(getConsumerIdKey(app, topic));
        if (idMap == null) {
            return size;
        }
        Iterator it = idMap.keySet().iterator();
        while (it.hasNext()) {
            Consumer consumer = consumers.get(it.next());
            //没有设置超时时间或者没有超时，认为是可用的consumer
            if (consumer != null && (consumer.getLastGetMessageTime() == 0
                    || (consumer.getLastGetMessageTime() + ackTimeout > SystemClock.getInstance().now()))) {
                size++;
            }
        }
        return size;
    }

    private String getConsumerIdKey(String app, String topic) {
        StringBuilder sb = new StringBuilder();
        sb.append(app).append("~@").append(topic);
        return sb.toString();
    }

    /**
     * 返回所有连接。
     *
     * @return 连接列表
     */
    public List<Connection> getConnection() {
        return new ArrayList<Connection>(connections.values());
    }

    /**
     * 获取链接
     *
     * @param id 链接id
     * @return 连接
     */
    public Connection getConnectionById(final String id) {
        return connections.get(id);
    }

    /**
     * 添加consumer
     *
     * @param consumer 消费者
     * @return <li>true 成功</li>
     * <li>false 失败：消费者已经存在或连接不存在</li>
     */
    public boolean addConsumer(final Consumer consumer) {
        if (consumer == null) {
            return false;
        }
        // 添加到缓存
        if (consumers.putIfAbsent(consumer.getId(), consumer) == null) {
            addConsumerId(consumer);
            try {
                // 成功则同步通知监听器
                eventManager.inform(new SessionEvent(SessionEventType.AddConsumer, consumer));
            } catch (Exception e) {
                logger.error("通知增加消费者失败," + e.getMessage(), e);
                removeConsumer(consumer.getId());
                return false;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("add consumer :" + consumer.getId());
            }
            return true;
        } else {
            logger.warn(String.format("consumer is already exists. topic=%s app=%s id=%d", consumer.getTopic(),
                    consumer.getApp(), consumer.getId()));
        }
        return true;
    }

    /**
     * 删除consumer
     *
     * @param consumerId 消费者ID
     */
    public void removeConsumer(final String consumerId) {
        Consumer consumer = consumers.remove(consumerId);
        removeConsumerId(consumer);
        if (consumer != null) {
            // 异步通知
            eventManager.add(new SessionEvent(SessionEventType.RemoveConsumer, consumer));
        }
    }

    /**
     * 获取所有的consumer
     *
     * @return 消费者列表
     */
    public List<Consumer> getConsumer() {
        return new ArrayList<Consumer>(consumers.values());
    }

    /**
     * 获取所有的 Producers
     * @return 生产者列表
     */
    public List<Producer> getProducer() {
        return new ArrayList<Producer>(producers.values());
    }

    /**
     * 根据ID查找消费者
     *
     * @param id 消费者ID
     * @return 匹配的消费者
     */
    public Consumer getConsumerById(final String id) {
        return consumers.get(id);
    }

    /**
     * 添加生产者
     *
     * @param producer 生产者
     * @return <li>true 成功</li>
     * <li>false 失败:连接不存在或生产者已经存在</li>
     */
    public boolean addProducer(final Producer producer) {
        if (producer == null) {
            return false;
        }
        if (producers.putIfAbsent(producer.getId(), producer) == null) {
            try {
                eventManager.inform(new SessionEvent(SessionEventType.AddProducer, producer));
            } catch (Exception e) {
                removeProducer(producer.getId());
                logger.error("通知生产者增加失败," + e.getMessage(), e);
                return false;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("add producer" + producer.getId());
            }
            return true;
        } else {
            logger.warn(String.format("producer is already exists. topic=%s app=%s id=%d", producer.getTopic(),
                    producer.getApp(), producer.getId()));
        }
        return true;
    }

    /**
     * 删除生产者
     *
     * @param producerId 消费者ID
     */
    public void removeProducer(final String producerId) {
        Producer producer = producers.remove(producerId);
        if (producer != null) {
            eventManager.add(new SessionEvent(SessionEventType.RemoveProducer, producer));
        }
    }

    /**
     * 获取producer
     *
     * @param id 生产者ID
     * @return 匹配的生产者
     */
    public Producer getProducerById(final String id) {
        return this.producers.get(id);
    }

    /**
     * 添加事件监听器
     *
     * @param listener 监听器
     */
    public void addListener(final EventListener<SessionEvent> listener) {
        if (listener != null) {
            this.eventManager.addListener(listener);
        }
    }

    /**
     * 移除事件监听器
     *
     * @param listener 监听器
     */
    public void removeListener(final EventListener<SessionEvent> listener) {
        if (listener != null) {
            this.eventManager.removeListener(listener);
        }
    }

    /**
     * 关闭生产者连接
     *
     * @param topic 主题
     * @param app   应用
     */
    public void closeProducer(final String topic, final String app) {
        Set<String> connectionIds = new HashSet<String>(20);

        String connectionId;
        Connection connection;
        Transport transport;
        Producer producer;
        Map.Entry<String, Producer> entry;
        // 遍历生产者
        Iterator<Map.Entry<String, Producer>> it = producers.entrySet().iterator();
        while (it.hasNext()) {
            // 得到生产者
            entry = it.next();
            producer = entry.getValue();
            // 判断主题和应用
            if (producer.getTopic().equals(topic) && producer.getApp().equals(app)) {
                // 判断连接是否已经处理过
                connectionId = producer.getConnectionId();
                if (connectionIds.add(connectionId)) {
                    closeRelatedTransport(connectionId);
                }
            }
        }

    }

    /**
     * 关闭消费者连接
     *
     * @param topic 主题
     * @param app   应用
     */
    public void closeConsumer(final String topic, final String app) {
        Set<String> connectionIds = new HashSet<String>(20);

        Connection connection;
        Transport transport;
        String connectionId;
        Consumer consumer;
        Map.Entry<String, Consumer> entry;
        // 遍历消费者
        Iterator<Map.Entry<String, Consumer>> it = consumers.entrySet().iterator();
        while (it.hasNext()) {
            // 得到消费者
            entry = it.next();
            consumer = entry.getValue();
            // 判断主题和应用
            if ((topic == null || consumer.getTopic().equals(topic)) && (app == null || consumer.getApp().equals(app))) {
                // 判断连接是否已经处理过
                connectionId = consumer.getConnectionId();
                if (connectionIds.add(connectionId)) {
                    closeRelatedTransport(connectionId);

                }
            }
        }
    }

    private void closeRelatedTransport(String connectionId) {
        Connection connection;
        Transport transport;// 取到连接
        connection = connections.get(connectionId);
        if (connection != null) {
            // 关闭连接
            transport = connection.getTransport();
            if (transport != null) {
                // 关闭通道会广播事件，自动触发清理消费者和连接
                transport.stop();
            }
        }
    }

    public void checkAndBindSequentialProducer(String app, String topic, String producerId) throws JMQException {
        sequentialManager.checkAndBindProducer(app, topic, producerId);
    }

    protected void onRemoveProducer(final com.ipd.jmq.common.network.v3.session.Producer producer) throws JMQException {
        sequentialManager.unbindProducer(producer);
    }


    /**
     * 事件类型
     */
    public static enum SessionEventType {
        /**
         * 增加连接
         */
        AddConnection,
        /**
         * 删除连接
         */
        RemoveConnection,
        /**
         * 增加生产者
         */
        AddProducer,
        /**
         * 删除生产者
         */
        RemoveProducer,
        /**
         * 增加消费者
         */
        AddConsumer,
        /**
         * 增加消费者
         */
        RemoveConsumer,
    }

    /**
     * 会话事件
     */
    public static class SessionEvent {
        // 类型
        private SessionEventType type;
        // 连接
        private Connection connection;
        // 消费者
        private Consumer consumer;
        // 生产者
        private Producer producer;

        public SessionEvent(SessionEventType type, Connection connection) {
            this.type = type;
            this.connection = connection;
        }

        public SessionEvent(SessionEventType type, Consumer consumer) {
            this.type = type;
            this.consumer = consumer;
        }

        public SessionEvent(SessionEventType type, Producer producer) {
            this.type = type;
            this.producer = producer;
        }

        public SessionEventType getType() {
            return type;
        }

        public void setType(SessionEventType type) {
            this.type = type;
        }

        public Connection getConnection() {
            return connection;
        }

        public void setConnection(Connection connection) {
            this.connection = connection;
        }

        public Consumer getConsumer() {
            return consumer;
        }

        public void setConsumer(Consumer consumer) {
            this.consumer = consumer;
        }

        public Producer getProducer() {
            return producer;
        }

        public void setProducer(Producer producer) {
            this.producer = producer;
        }
    }

}