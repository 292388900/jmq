package com.ipd.jmq.client.connection;

import com.ipd.jmq.client.cluster.ClusterEvent;
import com.ipd.jmq.client.cluster.ClusterManager;
import com.ipd.jmq.client.stat.Trace;
import com.ipd.jmq.client.stat.TraceBuilder;
import com.ipd.jmq.common.cluster.BrokerCluster;
import com.ipd.jmq.common.cluster.BrokerGroup;
import com.ipd.jmq.common.cluster.Permission;
import com.ipd.jmq.common.network.v3.netty.NettyClient;
import com.ipd.jmq.common.network.v3.session.ClientId;
import com.ipd.jmq.common.network.FailoverState;
import com.ipd.jmq.common.network.RoundRobinFailoverPolicy;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.network.Ipv4;
import com.ipd.jmq.toolkit.retry.RetryPolicy;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 集群通道管理器
 */
public class ClusterTransportManager extends Service implements TransportManager {
    protected static final String PRODUCE = "topic.produce";
    protected static final String CONSUME = "topic.consume";
    private static final Logger logger = LoggerFactory.getLogger(ClusterTransportManager.class);
    // 传输通道配置
    protected TransportConfig config;
    // 集群管理器
    protected ClusterManager clusterManager;
    // netty 客户端
    protected NettyClient client;
    // 客户端应用ID
    protected ClientId clientId;
    // 重试策略
    protected RetryPolicy retryPolicy;
    // 集群状态变化监听器
    protected ClusterListener clusterListener = new ClusterListener();
    // 消费者主题传输通道集合
    protected ClientContainer readTransports = new ClientContainer(Permission.READ);
    // 生产者主题传输通道集合
    protected ClientContainer writeTransports = new ClientContainer(Permission.WRITE);
    // 性能跟踪器
    protected TraceBuilder traceBuilder;
    // 主题存储文件
    protected File topicFile;
    // 集群文件
    protected File clusterFile;
    // 用于判断主题是否变更
    protected String produceTopics;
    protected String consumeTopics;

    public ClusterTransportManager(TransportConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config can not be null!");
        }
        this.retryPolicy = new RetryPolicy(1000, 10 * 1000, 0, true, 1.2, 0);
        this.config = config;
        this.client = new NettyClient(config);
    }

    @Override
    protected void validate() throws Exception {
        super.validate();

        if (config.getApp() == null || config.getApp().isEmpty()) {
            throw new IllegalStateException("config.app can not be empty");
        }
        if (config.getUser() == null || config.getUser().isEmpty()) {
            throw new IllegalStateException("config.user can not be empty");
        }
        if (config.getPassword() == null || config.getPassword().isEmpty()) {
            throw new IllegalStateException("config.password can not be empty");
        }
        if (config.getAddress() == null || config.getAddress().isEmpty()) {
            throw new IllegalStateException("config.address can not be empty");
        }
        List<String> addresses = config.getAddresses();
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalStateException("config.address is invalid");
        }
        if (config.getTempPath() == null || config.getTempPath().isEmpty()) {
            config.setTempPath(System.getProperty("user.home"));
        }
        if (topicFile == null) {
            // 创建主题文件
            File path = new File(config.getTempPath().trim());
            if (!path.isDirectory()) {
                throw new IllegalStateException("config.tempPath is not a directory. " + path.getPath());
            }
            if (!path.exists()) {
                if (!path.mkdirs()) {
                    if (!path.exists()) {
                        throw new IllegalStateException("create directory error. " + path.getPath());
                    }
                }
            }
            if (!path.canRead() || !path.canWrite()) {
                throw new IllegalStateException("no permission to access directory. " + path.getPath());
            }
            topicFile = new File(path, "mq_topic_" + config.getApp() + ".properties");

        }
        if (clusterFile == null) {
            clusterFile = new File(topicFile.getParentFile(), "mq_cluster_" + config.getApp() + ".data");
        }

        if (clientId == null) {
            //初始化客户端ID
            //String version = this.getClass().getPackage().getImplementationVersion();
            String version = GetVersionUtil.getClientVersion();
            String ip = Ipv4.getLocalIp();
            clientId = new ClientId(version, ip, SystemClock.now());
        }

        if (clusterManager == null) {
            clusterManager = new ClusterManager(client, clientId, clusterFile);
        }

        // 性能监控
        if (traceBuilder == null) {
            traceBuilder = new TraceBuilder();
            traceBuilder.setClient(client);
            traceBuilder.setTransportManager(this);
        }

    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        // 从本地文件加载数据
        loadTopics();
        //启动netty客户端
        client.start();
        clusterManager.start();
        //启动集群管理器
        clusterManager.addListener(clusterListener);
        // 启动性能统计管理器
        traceBuilder.start();

        // 确保连接上
        logger.info("wait to connect to broker.");
        checkConnect();

        // 创建任务调度器
        logger.info("cluster transport manager is started");
    }

    /**
     * 确保连接上
     */
    protected void checkConnect() throws InterruptedException {
        Collection<String> topics = writeTransports.getTopics();
        if (topics == null || topics.isEmpty()) {
            return;
        }
        // 没有连接上的主题
        List<String> none = new ArrayList<String>(topics);
        int connectionTimeout = config.getConnectionTimeout() + 1000;
        int count = connectionTimeout / 100;
        List<GroupTransport> transports;
        String topic;
        boolean flag;
        // 循环次数
        for (int i = 0; i < count; i++) {
            // 休息100毫秒
            Thread.sleep(100);
            // 倒序遍历没有连接上的主题，判断是否连接上
            for (int j = none.size() - 1; j >= 0; j--) {
                topic = none.get(j);
                flag = false;
                transports = writeTransports.get(topic);
                if (transports != null && !transports.isEmpty()) {
                    // 确保该分组有一个连接可用
                    for (GroupTransport transport : transports) {
                        if (transport.getState() == FailoverState.CONNECTED) {
                            flag = true;
                            break;
                        }
                    }
                    if (flag) {
                        none.remove(j);
                    }
                } else {
                    none.remove(j);
                }
            }
            // 没有未连接上的主题
            if (none.isEmpty()) {
                break;
            }
        }
        if (!none.isEmpty()) {
            logger.info(String.format("there is %d topics which is not connected to broker,%s", none.size(),
                    join(none, ',')));
        }

    }


    @Override
    protected void doStop() {
        super.doStop();

        if (clusterManager != null) {
            clusterManager.removeListener(clusterListener);
            clusterManager.stop();
        }
        Close.close(traceBuilder);

        // 必须在清理之前把主题保存到文件
        writeTopics();
        readTransports.clear();
        writeTransports.clear();
        client.stop();
        logger.info("cluster transport manager is stopped");
    }

    /**
     * 加载主题历史
     */
    protected void loadTopics() {
        FileReader reader = null;
        try {
            if (topicFile.exists()) {
                Properties properties = new Properties();
                reader = new FileReader(topicFile);
                properties.load(reader);
                String text = properties.getProperty(PRODUCE);
                if (text != null) {
                    writeTransports.add(text.split("[,;]"));
                    produceTopics = text;
                }
                text = properties.getProperty(CONSUME);
                if (text != null) {
                    readTransports.add(text.split("[,;]"));
                    consumeTopics = text;
                }
            }
        } catch (FileNotFoundException ignored) {
        } catch (IOException ignored) {
        } catch (AccessControlException e) {
            logger.error("no permission to read file," + topicFile.getPath());
        } finally {
            Close.close(reader);
        }
    }

    /**
     * 记录主题历史
     */
    protected void writeTopics() {
        FileWriter writer = null;
        try {
            if (!topicFile.exists()) {
                if (!topicFile.createNewFile()) {
                    if (!topicFile.exists()) {
                        throw new FileNotFoundException("create file error.");
                    }
                }
            }
            String pTopics = join(writeTransports.getTopics(), ',');
            String cTopics = join(readTransports.getTopics(), ',');
            if (!pTopics.equals(produceTopics) || !cTopics.equals(consumeTopics)) {
                produceTopics = pTopics;
                consumeTopics = cTopics;
                Properties properties = new Properties();
                properties.setProperty(PRODUCE, produceTopics);
                properties.setProperty(CONSUME, consumeTopics);
                writer = new FileWriter(topicFile);
                properties.store(writer, "topics");
                writer.flush();
            }
        } catch (FileNotFoundException ignored) {
            logger.error("no permission to create file," + topicFile.getPath());
        } catch (IOException ignored) {
        } catch (AccessControlException e) {
            logger.error("no permission to access file," + topicFile.getPath());
        } finally {
            Close.close(writer);
        }
    }

    /**
     * 连接字符串
     *
     * @param values    字符串集合
     * @param delimiter 分隔符
     * @return 字符串
     */
    protected String join(Collection<String> values, char delimiter) {
        if (values == null || values.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        int count = 0;
        for (String topic : values) {
            if (topic != null && !topic.isEmpty()) {
                if (count++ > 0) {
                    builder.append(delimiter);
                }
                builder.append(topic);
            }
        }
        return builder.toString();
    }

    @Override
    public TransportConfig getConfig() {
        return config;
    }

    public NettyClient getClient() {
        return client;
    }

    @Override
    public Trace getTrace() {
        return traceBuilder;
    }

    @Override
    public GroupClient getTransport(BrokerGroup group, String topic, Permission permission) {
        if (permission == Permission.READ) {
            return readTransports.get(topic, group);
        } else if (permission == Permission.WRITE) {
            return writeTransports.get(topic, group);
        }
        return null;
    }

    @Override
    public void removeTransport(BrokerGroup group, String topic, Permission permission) {
        if (permission == Permission.READ) {
            readTransports.remove(topic, group);
        } else if (permission == Permission.WRITE) {
            writeTransports.remove(topic, group);
        }
    }

    @Override
    public List<GroupTransport> getTransports(String topic, Permission permission) {
        if (permission == Permission.READ) {
            return readTransports.get(topic);
        } else if (permission == Permission.WRITE) {
            return writeTransports.get(topic);
        }
        return new ArrayList<GroupTransport>();
    }

    @Override
    public void removeTransports(String topic, Permission permission) {
        if (permission == Permission.READ) {
            readTransports.remove(topic);
        } else if (permission == Permission.WRITE) {
            writeTransports.remove(topic);
        }
    }

    @Override
    public ClusterManager getClusterManager() {
        return clusterManager;
    }

    @Override
    public void addListener(String topic, EventListener<ClusterEvent> listener) {
        if (readTransports.add(topic)) {
            clusterManager.addListener(listener);
        }
    }

    @Override
    public void removeListener(String topic, EventListener<ClusterEvent> listener) {
        if (readTransports.remove(topic)) {
            clusterManager.removeListener(listener);
        }
    }

    /**
     * 集群变化监听器，负责处理传输通道相关部分
     */
    protected class ClusterListener implements EventListener<ClusterEvent> {
        @Override
        public void onEvent(ClusterEvent event) {
            // 加锁防止在关闭的过程中又产生新的连接
            synchronized (this) {
                try {
                    // 判断是否关闭
                    if (!isStarted()) {
                        return;
                    }
                    // 没用生产也没用消费
                    String topic = event.getTopic();
                    BrokerGroup group = event.getGroup();
                    if (!readTransports.contains(topic) && !writeTransports.contains(topic)) {
                        return;
                    }
                    switch (event.getType()) {
                        case ADD_BROKER:
                            // 增加broker
                            readTransports.add(topic, group);
                            writeTransports.add(topic, group);
                            logger.info(String.format("add broker %s for topic %s", group, topic));
                            break;
                        case REMOVE_BROKER:
                            // 移除broker
                            readTransports.remove(topic, group);
                            writeTransports.remove(topic, group);
                            logger.info(String.format("remove broker %s for topic %s", group, topic));
                            break;
                        case UPDATE_BROKER:
                            // 修改了权限
                            if (group.getPermission().contain(Permission.READ)) {
                                // 当前有读权限
                                readTransports.add(topic, group);
                            } else {
                                // 当前没有读权限
                                readTransports.remove(topic, group);
                            }
                            if (group.getPermission().contain(Permission.WRITE)) {
                                // 当前有写权限
                                writeTransports.add(topic, group);
                            } else {
                                // 当前无写权限
                                writeTransports.remove(topic, group);
                            }
                            logger.info(String.format("update broker %s for topic %s, permission %s", group, topic,
                                    group.getPermission()));
                            break;
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

    }

    /**
     * 集群连接容器
     */
    protected class ClientContainer {
        ConcurrentMap<String, ConcurrentMap<BrokerGroup, GroupClient>> transports =
                new ConcurrentHashMap<String, ConcurrentMap<BrokerGroup, GroupClient>>();
        Permission permission;

        public ClientContainer(Permission permission) {
            this.permission = permission;
        }

        /**
         * 获取分组连接映射，会自动创建连接
         *
         * @param topic 主题
         * @return 分组连接映射
         */
        protected ConcurrentMap<BrokerGroup, GroupClient> getGroups(final String topic) {
            ConcurrentMap<BrokerGroup, GroupClient> map = getAndCreate(topic);
            // 初始化
            if (map.isEmpty()) {
                BrokerCluster cluster = clusterManager.getCluster(topic);
                if (cluster != null) {
                    List<BrokerGroup> groups = cluster.getGroups();
                    if (groups != null) {
                        for (BrokerGroup bg : groups) {
                            add(topic, bg);
                        }
                    }
                }
            }
            return map;
        }

        /**
         * 根据主题获取散列对象
         *
         * @param topic 主题
         * @return 散列对象
         */
        protected ConcurrentMap<BrokerGroup, GroupClient> getAndCreate(final String topic) {
            if (topic == null || topic.isEmpty()) {
                return null;
            }
            ConcurrentMap<BrokerGroup, GroupClient> map = transports.get(topic);
            if (map == null) {
                map = new ConcurrentHashMap<BrokerGroup, GroupClient>();
                ConcurrentMap<BrokerGroup, GroupClient> old = transports.putIfAbsent(topic, map);
                if (old != null) {
                    map = old;
                }
            }
            return map;
        }

        /**
         * 生产者或消费者获取连接
         * permission
         *
         * @param topic 主题
         * @param group 集群分组
         * @return 连接
         */
        public GroupClient get(final String topic, final BrokerGroup group) {
            ConcurrentMap<BrokerGroup, GroupClient> map = getGroups(topic);
            return map.get(group);
        }

        /**
         * 生产者或消费者获取连接
         *
         * @param topic 主题
         * @return 连接列表
         */
        public List<GroupTransport> get(final String topic) {
            List<GroupTransport> transports = new ArrayList<GroupTransport>(10);
            transports.addAll(getGroups(topic).values());
            return transports;
        }

        /**
         * 获取主题
         *
         * @return 主题集合
         */
        public Collection<String> getTopics() {
            return transports.keySet();
        }

        /**
         * 创建连接
         *
         * @param topic 主题
         * @param group 分组
         * @return 成功标示
         */
        public boolean add(final String topic, final BrokerGroup group) {
            if (!group.getPermission().contain(permission)) {
                return false;
            }
            ConcurrentMap<BrokerGroup, GroupClient> groups = transports.get(topic);
            // 如果groups不存在，则表示没有订阅或生产该消息
            if (groups != null && !groups.containsKey(group)) {
                // 新增加分组
                GroupClient transport =
                        permission == Permission.READ ?
                                new ConsumerClient(client, new GroupFailoverPolicy(group, Permission.READ), retryPolicy)
                                : new ProducerClient(client, new GroupFailoverPolicy(group, Permission.WRITE), retryPolicy);
                transport.setGroup(group);
                transport.setClientId(clientId);
                transport.setConfig(config);
                transport.setTopic(topic);
                transport.setDataCenter(clusterManager.getDataCenter());
                try {
                    // 由消费线程去启动
                    if (permission != Permission.READ) {
                        transport.start();
                    }
                    GroupClient old = groups.putIfAbsent(group, transport);
                    if (old != null) {
                        // 分组连接已经存在，则判断是否要修改权重
                        old.setWeight(group.getWeight());
                        transport.stop();
                    } else {
                        return true;
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            } else if (groups != null) {
                //判断权重是否发生变更
                GroupClient transport = groups.get(group);
                if (transport != null) {
                    transport.setWeight(group.getWeight());
                }
            }
            return false;
        }

        /**
         * 是否包含主题
         *
         * @param topic 主题
         * @return 是否存在主题
         */
        public boolean contains(String topic) {
            return transports.containsKey(topic);
        }

        /**
         * 添加主题
         *
         * @param topic 主题
         * @return 成功标示
         */
        public boolean add(String topic) {
            return getAndCreate(topic) != null;
        }

        /**
         * 添加主题
         *
         * @param topics 主题
         */
        public void add(String[] topics) {
            if (topics == null || topics.length == 0) {
                return;
            }
            for (String topic : topics) {
                add(topic);
            }
        }

        /**
         * 移除主题
         *
         * @param topic 主题
         * @return 成功标示
         */
        public boolean remove(String topic) {
            ConcurrentMap<BrokerGroup, GroupClient> map = transports.remove(topic);
            if (map != null) {
                for (GroupClient transport : map.values()) {
                    transport.stop();
                }
                map.clear();
                return true;
            }
            return false;
        }

        /**
         * 移除Broker
         *
         * @param topic 主题
         * @param group 分组
         * @return 成功标示
         */
        protected boolean remove(String topic, BrokerGroup group) {
            ConcurrentMap<BrokerGroup, GroupClient> map = transports.get(topic);
            if (map != null) {
                GroupClient transport = map.remove(group);
                if (transport != null) {
                    transport.stop();
                    return true;
                }
            }
            return false;
        }

        /**
         * 清除连接
         */
        public void clear() {
            for (Map.Entry<String, ConcurrentMap<BrokerGroup, GroupClient>> entry : transports.entrySet()) {
                for (GroupClient transport : entry.getValue().values()) {
                    transport.stop();
                }
            }
            transports.clear();
        }
    }
}