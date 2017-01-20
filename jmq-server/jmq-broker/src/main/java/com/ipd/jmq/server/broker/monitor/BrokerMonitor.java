package com.ipd.jmq.server.broker.monitor;

import com.alibaba.fastjson.JSON;
import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.monitor.*;
import com.ipd.jmq.common.network.v3.session.Connection;
import com.ipd.jmq.common.network.v3.session.Consumer;
import com.ipd.jmq.common.network.v3.session.Producer;
import com.ipd.jmq.replication.ReplicationMaster;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.SessionManager;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
import com.ipd.jmq.server.broker.monitor.impl.*;
import com.ipd.jmq.server.broker.profile.PubSubStat;
//import com.ipd.jmq.server.broker.retry.RetryManager;
import com.ipd.jmq.server.store.Store;
import com.ipd.jmq.server.store.StoreConfig;

import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.network.Ipv4;
import com.ipd.jmq.toolkit.service.ServiceThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 完成监控统计及输出指令（获取状态信息或完成某种操作）
 * @author xuzhenhua
 */
public class BrokerMonitor extends BrokerMonitorTool {

    private static final Logger logger = LoggerFactory.getLogger(BrokerMonitor.class);

    // 会话管理
    protected SessionManager sessionManager;
    // 集群管理器
    protected ClusterManager clusterManager;
    // 派发管理器
    public DispatchService dispatchService;
//    // 重试管理
//    protected RetryManager retryManager;
    // 配置
    protected BrokerConfig brokerConfig;
    // 存储配置
    protected StoreConfig storeConfig;
    // 存储
    protected Store store;
    // 会话监听器
    protected SessionListener sessionListener = new SessionListener();
    // 性能统计插件
    protected Thread perfStatThread;
    // 复制主分片
    protected ReplicationMaster replicationMaster;
    // 客户端性能统计
    protected PubSubStat pubSubStat = new PubSubStat();
    // 统计连接数
    protected ConcurrentHashMap<String, Client> connections = new ConcurrentHashMap<String, Client>();
    // 当前Broker
    protected Broker broker;
    // 统计基础汇总信息
    protected BrokerStat brokerStat;
    // 统计性能
    protected BrokerPerfBuffer brokerPerfBuffer;
    // 异常统计
    protected BrokerExeBuffer brokerExeBuffer;
    // 重试性能统计
    protected RetryPerfBuffer retryPerfBuffer;
//  RetryManager retryManager,
    public BrokerMonitor(SessionManager sessionManager, ClusterManager clusterManager, DispatchService dispatchService,BrokerConfig config, ReplicationMaster replicationMaster,long brokerStartTime) {
        if (sessionManager == null) {
            throw new IllegalArgumentException("sessionManager can not be null");
        }
        if (clusterManager == null) {
            throw new IllegalArgumentException("clusterManager can not be null");
        }
        if (dispatchService == null) {
            throw new IllegalArgumentException("dispatchService can not be null");
        }
//        if (retryManager == null) {
//            throw new IllegalArgumentException("retryManager can not be null");
//        }

        this.sessionManager = sessionManager;
        this.clusterManager = clusterManager;
        this.dispatchService = dispatchService;
        this.brokerConfig = config;

        this.storeConfig = config.getStoreConfig();
        this.store = config.getStore();
        this.replicationMaster = replicationMaster;
        this.retryPerfBuffer = new RetryPerfBuffer();
        this.broker = clusterManager.getBroker();
        this.brokerStat = new BrokerStat(broker.getName());
        this.brokerPerfBuffer = new BrokerPerfBuffer(broker.getName(), broker.getGroup());
        this.brokerExeBuffer = new BrokerExeBuffer(broker.getName(), broker.getGroup());
        this.brokerStartTime = brokerStartTime;

        this.partitionMonitor = new PartitionMonitorImpl.Builder(store, brokerConfig).clusterManager(clusterManager).
                dispatchService(dispatchService).replicationMaster(replicationMaster).
                brokerStat(brokerStat).brokerExeBuffer(brokerExeBuffer).brokerPerfBuffer(brokerPerfBuffer).
                pubSubStat(pubSubStat).broker(broker).brokerStartTime(brokerStartTime).build();
        this.topicMonitor = new TopicMonitorImpl.Builder(pubSubStat, retryPerfBuffer).build();
        this.sessionMonitor = new SessionMonitorImpl.Builder(connections, sessionManager).
                broker(broker).brokerStat(brokerStat).build();
        this.producerMonitor = new ProducerMonitorImpl.Builder().build();
        this.consumerMonitor = new ConsumerMonitorImpl.Builder(store, clusterManager, dispatchService).build();
    }

    @Override
    protected void validate() throws Exception {
        super.validate();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        // 加载数据
        load();
        sessionManager.addListener(sessionListener);
        // 性能切片
        perfStatThread = new Thread(new ServiceThread(this) {
            @Override
            protected void execute() throws Exception {
                // 性能切片
                brokerPerfBuffer.slice();
                //异常数据切片,不持久化
                brokerPerfBuffer.slice();

                retryPerfBuffer.slice();
                // 持久化数据
                save();
            }

            @Override
            public long getInterval() {
                return brokerConfig.getPerfStatInterval();
            }

            @Override
            public boolean onException(Throwable e) {
                logger.error(e.getMessage(), e);
                return true;
            }
        }, "JMQ_SERVER_PERFORMANCE_STAT");
        perfStatThread.start();
        clusterManager.addListener(null);
        logger.info("BrokerMonitor is started");
    }

    @Override
    protected void doStop() {
        sessionManager.removeListener(sessionListener);
        if (perfStatThread != null) {
            perfStatThread.interrupt();
            perfStatThread = null;
        }
        logger.info("BrokerMonitor is stopped");
        super.doStop();
    }

    // 持久化统计数据
    protected void save() {
        FileWriter writer = null;
        try {
            writer = new FileWriter(storeConfig.getStatFile());

            Map<String, Object> brokerStatMap = new HashMap<String, Object>();
            Map<String, Map<String, Object>> topicStats = new HashMap<String, Map<String, Object>>();
            brokerStatMap.put("enQueue", brokerStat.getEnQueue());
            brokerStatMap.put("enQueueSize", brokerStat.getEnQueueSize());
            brokerStatMap.put("deQueue", brokerStat.getDeQueue());
            brokerStatMap.put("deQueueSize", brokerStat.getDeQueueSize());
            brokerStatMap.put("topicStats", topicStats);

            Map<String, Object> topicStatMap;
            TopicStat topicStat;
            AppStat appStat;
            Map<String, Object> appStatMap;
            Map<String, Map<String, Object>> appStats;
            for (Map.Entry<String, TopicStat> entry : brokerStat.getTopicStats().entrySet()) {
                topicStat = entry.getValue();
                topicStatMap = new HashMap<String, Object>();
                topicStatMap.put("enQueue", topicStat.getEnQueue());
                topicStatMap.put("enQueueSize", topicStat.getEnQueueSize());
                topicStatMap.put("deQueue", topicStat.getDeQueue());
                topicStatMap.put("deQueueSize", topicStat.getDeQueueSize());
                appStats = new HashMap<String, Map<String, Object>>();
                topicStatMap.put("appStats", appStats);
                for (Map.Entry<String, AppStat> e : topicStat.getAppStats().entrySet()) {
                    appStat = e.getValue();
                    appStatMap = new HashMap<String, Object>();
                    appStatMap.put("enQueue", appStat.getEnQueue());
                    appStatMap.put("enQueueSize", appStat.getEnQueueSize());
                    appStatMap.put("deQueue", appStat.getDeQueue());
                    appStatMap.put("deQueueSize", appStat.getDeQueueSize());
                    appStatMap.put("consumerRole", appStat.isConsumerRole());
                    appStatMap.put("producerRole", appStat.isProducerRole());
                    appStats.put(e.getKey(), appStatMap);
                }
                topicStats.put(entry.getKey(), topicStatMap);
            }
            writer.write(JSON.toJSONString(brokerStatMap));
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        } finally {
            Close.close(writer);
        }
    }

    // 加载本地统计数据
    protected void load() {
        if (!storeConfig.getStatFile().exists()) {
            return;
        }
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(storeConfig.getStatFile()));
            String line;
            StringBuilder builder = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                builder.append(line).append('\n');
            }
            BrokerStat stat = JSON.parseObject(builder.toString(), BrokerStat.class);
            if (stat != null) {
                stat.setName(broker.getName());
                for (Map.Entry<String, TopicStat> entry : stat.getTopicStats().entrySet()) {
                    String Topic = entry.getKey();
                    TopicStat topicStat = entry.getValue();
                    topicStat.setTopic(Topic);
                    for (Map.Entry<String, AppStat> appEntry : topicStat.getAppStats().entrySet()) {
                        String app = appEntry.getKey();
                        AppStat appStat = appEntry.getValue();
                        appStat.setApp(app);
                    }
                }
                brokerStat = stat;
            }
        } catch (Throwable e) {
            logger.error("load broker stat error.", e);
        } finally {
            Close.close(reader);
        }
    }

    /**
     * 生产者统计 begin
     */

    // 产生消息时触发
    public void onPutMessage(final String topic, final String app, final int count, final int size, final int time) {

        if (topic == null || topic.isEmpty() || app == null || app.isEmpty()) {
            return;
        }

        brokerStat.getEnQueue().getAndAdd(count);
        brokerStat.getEnQueueSize().addAndGet(size);

        TopicStat topicStat = brokerStat.getAndCreateTopicStat(topic);
        topicStat.getEnQueue().getAndAdd(count);
        topicStat.getEnQueueSize().addAndGet(size);

        AppStat appStat = topicStat.getAndCreateAppStat(app);
        appStat.getEnQueue().getAndAdd(count);
        appStat.getEnQueueSize().addAndGet(size);

        brokerPerfBuffer.putMessage(topic, app, count, size, time);

        //metics数据
        metricProducer(topic, app, count, size, time);
    }

    private void metricProducer(final String topic, final String app, final int count, final long size, final int time) {
        pubSubStat.success(topic, app, PubSubStat.StatType.produce, count, size, time);
    }

    // 产生消息时发生异常时触发
    public void onSendFailure() {
        brokerExeBuffer.onSendFailure();
    }

    /**
     * 生产者统计 end
     */


    /**
     * 消费者统计 end
     */
    // 消费消息时触发计数
    public void onGetMessage(final String topic, final String app, final int count, final long size, final int time) {
        if (topic == null || topic.isEmpty() || app == null || app.isEmpty()) {
            return;
        }
        brokerStat.getDeQueue().getAndAdd(count);
        brokerStat.getDeQueueSize().addAndGet(size);

        TopicStat topicStat = brokerStat.getAndCreateTopicStat(topic);
        topicStat.getDeQueue().getAndAdd(count);
        topicStat.getDeQueueSize().addAndGet(size);

        AppStat appStat = topicStat.getAndCreateAppStat(app);
        appStat.getDeQueue().getAndAdd(count);
        appStat.getDeQueueSize().addAndGet(size);

        brokerPerfBuffer.getMessage(topic, app, count, size, time);

        //消费统计
        metricConsumer(topic, app, count, size, time);
    }

    private void metricConsumer(final String topic, final String app, final int count, final long size, final int
            time) {
        pubSubStat.success(topic, app, PubSubStat.StatType.consume, count, size, time);
    }

    // 消费者过期统计
    public void onCleanExpire(ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> expireStats) {
        if (expireStats == null || expireStats.isEmpty()) {
            return;
        }

        Set<String> topics = expireStats.keySet();
        for (String topic : topics) {
            ConcurrentMap<String, AtomicInteger> appMapping = expireStats.get(topic);
            Set<String> apps = appMapping.keySet();
            for (String app : apps) {
                brokerExeBuffer.onCleanExpire(app, topic, appMapping.get(app).get());
            }
        }

    }

    /**
     * 消费者统计 end
     */

    /**
     * 重试统计 begin
     */
    public void onRetryError(String topic, String app, long count) {
        retryPerfBuffer.onRetryError(topic, app, count);
    }

    public void onAddRetry(String topic, String app, long count) {
        retryPerfBuffer.onAddRetry(topic, app, count);

    }

    public void onRetrySuccess(String topic, String app, long count) {
        retryPerfBuffer.onRetrySuccess(topic, app, count);
    }
    /**
     * 重试统计 end
     */


    /**
     * 会话统计 begin
     */
    public void onAddConnection(final Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            InetSocketAddress address = Ipv4.toAddress(connection.getAddress());
            Client client = new Client();
            client.setConnectionId(connection.getId());
            client.setApp(connection.getApp());
            client.setLanguage(connection.getLanguage().name());
            client.setVersion(connection.getVersion());
            client.setIp(address.getAddress().getHostAddress());
            client.setPort(address.getPort());

            if (connections.putIfAbsent(client.getConnectionId(), client) == null) {
                brokerStat.getConnection().getAndIncrement();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    // 移除连接时触发
    public void onRemoveConnection(final Connection connection) {
        if (connection == null) {
            return;
        }
        // 移除连接
        if (connections.remove(connection.getId()) != null) {
            brokerStat.getConnection().decrementAndGet();
        }

        AppStat appStat;
        ConcurrentMap<String, Client> clients;
        // 遍历所有的主题
        for (TopicStat topicStat : brokerStat.getTopicStats().values()) {
            // 获取应用统计
            appStat = topicStat.getAppStats().get(connection.getApp());
            if (appStat != null) {
                // 应用存在，判断链接是否存在
                clients = appStat.getClients();
                if (clients.remove(connection.getId()) != null) {
                    appStat.getConnection().decrementAndGet();
                }
            }
        }

        brokerExeBuffer.removeConnection(connection.getApp());
    }

    //新增生产者时触发
    public void onAddProducer(final Producer producer) {
        if (producer == null) {
            return;
        }
        TopicStat topicStat = brokerStat.getAndCreateTopicStat(producer.getTopic());
        topicStat.getProducer().incrementAndGet();

        AppStat appStat = topicStat.getAndCreateAppStat(producer.getApp());
        appStat.setProducerRole(true);
        appStat.getProducer().incrementAndGet();

        // 增加连接数
        Client client = connections.get(producer.getConnectionId());
        if (client == null) {
            return;
        }
        client.setProducerRole(true);
        if (appStat.getClients().putIfAbsent(producer.getConnectionId(), client) == null) {
            appStat.getConnection().incrementAndGet();
        }
    }

    // 移除生产者时触发
    public void onRemoveProducer(final Producer producer) {
        if (producer == null) {
            return;
        }
        TopicStat topicStat = brokerStat.getTopicStats().get(producer.getTopic());
        if (topicStat != null) {
            topicStat.getProducer().decrementAndGet();
            AppStat appStat = topicStat.getAppStats().get(producer.getApp());
            if (appStat != null) {
                appStat.getProducer().decrementAndGet();
            }
        }

        brokerExeBuffer.removeProducer(producer.getApp(), producer.getTopic());
    }

    // 添加消费者时触发
    public void onAddConsumer(final Consumer consumer) {
        if (consumer == null) {
            return;
        }
        TopicStat topicStat = brokerStat.getAndCreateTopicStat(consumer.getTopic());
        topicStat.getConsumer().incrementAndGet();

        AppStat appStat = topicStat.getAndCreateAppStat(consumer.getApp());
        appStat.setConsumerRole(true);
        appStat.getConsumer().incrementAndGet();

        // 增加连接数
        Client client = connections.get(consumer.getConnectionId());
        if (client == null) {
            return;
        }
        client.setConsumerRole(true);
        if (appStat.getClients().putIfAbsent(consumer.getConnectionId(), client) == null) {
            appStat.getConnection().incrementAndGet();
        }

    }

    // 移除消费者时触发
    public void onRemoveConsumer(final Consumer consumer) {
        if (consumer == null) {
            return;
        }
        TopicStat topicStat = brokerStat.getTopicStats().get(consumer.getTopic());
        if (topicStat != null) {
            topicStat.getConsumer().decrementAndGet();
            AppStat appStat = topicStat.getAppStats().get(consumer.getApp());
            if (appStat != null) {
                appStat.getConsumer().decrementAndGet();
            }
        }

        brokerExeBuffer.removeConsumer(consumer.getApp(), consumer.getTopic());
    }

    /**
     * 会话管理
     */
    protected class SessionListener implements EventListener<SessionManager.SessionEvent> {

        @Override
        public void onEvent(final SessionManager.SessionEvent event) {
            if (!isStarted()) {
                return;
            }
            switch (event.getType()) {
                case AddConnection:
                    onAddConnection(event.getConnection());
                    break;
                case RemoveConnection:
                    onRemoveConnection(event.getConnection());
                    break;
                case AddConsumer:
                    onAddConsumer(event.getConsumer());
                    break;
                case RemoveConsumer:
                    onRemoveConsumer(event.getConsumer());
                    break;
                case AddProducer:
                    onAddProducer(event.getProducer());
                    break;
                case RemoveProducer:
                    onRemoveProducer(event.getProducer());
                    break;
            }
        }
    }

    /**
     * 会话统计 end
     */
}