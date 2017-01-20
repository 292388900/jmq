package com.ipd.jmq.server.broker.cluster;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ipd.jmq.common.cluster.*;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.SessionManager;
import com.ipd.jmq.server.broker.dispatch.DispatchEvent;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
import com.ipd.jmq.server.broker.monitor.BrokerMonitor;
import com.ipd.jmq.registry.PathData;
import com.ipd.jmq.registry.Registry;
import com.ipd.jmq.registry.RegistryException;
import com.ipd.jmq.registry.listener.PathEvent;
import com.ipd.jmq.registry.listener.PathListener;
import com.ipd.jmq.registry.util.Path;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.lang.Charsets;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.service.ServiceThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 顺序消息管理器.
 *
 * @author lindeqiang
 * @since 2016/6/24 8:37
 */
public class SequentialManager extends Service {
    private static final Logger logger = LoggerFactory.getLogger(SequentialManager.class);
    private static final int SEQUENTIAL_QUEUE_SIZE = 1000;
    public static final String BROKER_MONITOR_URL = "com.ipd.jmq.server.jmx:broker=%s,monitor=BrokerMonitor";
    // Broker配置
    private BrokerConfig config;
    // 注册中心
    private Registry registry;
    // JMX 连接超时时间
    private long jmxConnectionTimeout = 3000L;
    // 顺序消息状态更新的时间间隔
    private long updateSequentialTopicInterval = 30 * 1000L;
    //顺序消息状态更新线程
    private Thread updateSequentialThread;
    // 集群管理器
    private ClusterManager clusterManager;
    // broker监听器
    private BrokerMonitor brokerMonitor;
    // 会话管理器
    private SessionManager sessionManager;
    // 消息派发服务
    private DispatchService dispatchService;
    // 消息派发服务监听器
    private DispatchListener dispatchListener = new DispatchListener();
    // 顺序消息监听器
    private PathListener sequentialTopicListener = new SequentialTopicListener();

    // 顺序消息消费和生产者状态
    private ConcurrentMap<String, SequentialTopicState> topicStates = new ConcurrentHashMap<String, SequentialTopicState>(SEQUENTIAL_QUEUE_SIZE);

    // 顺序消息的应用和生产者的绑定关系
    private ConcurrentMap<String, ConcurrentMap<String, String>> sequentialProducer = new ConcurrentHashMap<String, ConcurrentMap<String, String>>();


    public SequentialManager(BrokerConfig config) {
        this.config = config;
        this.registry = config.getRegistry();
    }

    @Override
    public void validate() throws Exception {
        if (brokerMonitor == null) {
            throw new IllegalStateException("broker monitor can not be null!");
        }
        if (dispatchService == null) {
            throw new IllegalStateException("dispatch service can not be null!");
        }
        if (clusterManager == null) {
            throw new IllegalStateException("cluster manager can not be null!");
        }
    }

    @Override
    public void doStart() throws Exception {
        updateSequentialThread = new Thread(new SequentialStateTask(this), "JMQ_SERVER_UPDATE_SEQUENTIAL_TASK");
        updateSequentialThread.setDaemon(true);
        updateSequentialThread.start();

        dispatchService.addListener(dispatchListener);
    }

    public void doStop() {
        if (updateSequentialThread != null) {
            updateSequentialThread.interrupt();
        }
        updateSequentialThread = null;
    }


    /**
     * 更新主题配置信息并向顺序节点添加监听器
     *
     * @param topicConfig
     */
    public void updateTopicStateAndAddListener(final TopicConfig topicConfig) {
        try {
            if (topicConfig == null || !topicConfig.checkSequential()) {
                return;
            }
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Sequential topic update fired!topic=%s", topicConfig.getTopic()));
            }
            String topic = topicConfig.getTopic();
            //更新主题信息
            getAndUpdateTopicState(topicConfig);
            //添加监听
            addTopicStateListener(topic);
        } catch (Exception e) {
            logger.error(String.format("Update sequential data %s error.", config.getTopicPath()), e);
        }
    }

    // 获取并更新顺序主题信息
    private void getAndUpdateTopicState(TopicConfig topicConfig) throws RegistryException, IOException {
        if (topicConfig == null) {
            return;
        }
        logger.info(String.format("Start get and update sequential topic state!topic=%s", topicConfig.getTopic()));

        String topic = topicConfig.getTopic();
        String topicPath = Path.concat(config.getSequentialPath(), topic);
        PathData pathData = registry.getData(topicPath);
        if (pathData != null && pathData.getData() != null && pathData.getData().length != 0) {
            updateTopicState(topicConfig, pathData.getData(), pathData.getVersion());
        }
    }

    // 向顺序节点添加监听器
    private void addTopicStateListener(String topic) {
        if (topic == null) {
            return;
        }
        //添加监听器 /jmq/sequential/topic
        registry.addListener(Path.concat(config.getSequentialPath(), topic), sequentialTopicListener);
    }

    //更新顺序节点信息
    private void updateTopicState(final TopicConfig topicConfig, byte[] data, int version) throws IOException {
        if (topicConfig == null || data == null || data.length == 0 || topicConfig.getTopic() == null ||
                topicConfig.getTopic().isEmpty()) {
            return;
        }

        String topic = topicConfig.getTopic();
        SequentialTopicState topicState = getAndCreateTopicState(topic);
        if (version != 0 && topicState != null && topicState.getVersion() > version) {
            logger.info(String.format("Topic %s were updated! originVersion=%s, currentVersion=%s", topic, topicState.getVersion(), version));
            return;
        }
        String content = new String(data, Charset.forName("UTF-8"));

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("New sequential topicState=%s", content));
        }
        SequentialTopicState newState = JSON.parseObject(content, SequentialTopicState.class);
        if (newState != null) {
            newState.setVersion(version);

            if (removeUnassignedBroker(topicConfig, newState)) {
                logger.info(String.format("groups changed try to sync sequential topic state!", topic));
                syncAndResetTopicState(newState);
            } else {
                resetTopicState(newState);
            }
        }
    }

    //将当前不再分配给topic的Broker强制从顺序Broker列表中移除
    private boolean removeUnassignedBroker(final TopicConfig topicConfig, final SequentialTopicState topicState) {
        if (topicState == null || topicConfig == null) {
            return false;
        }

        // 主题当前的分组
        Set<String> groups = topicConfig.getGroups();

        SequentialBrokerState brokerState = topicState.getBrokerState();
        if (groups == null || groups.isEmpty()) {
            //移除所有的Broker
            return topicState.removeAllBrokers();
        } else {
            // 找出已经被移除的Broker，再将其移除
            Deque<SequentialBrokerState.SequentialBroker> brokers = brokerState.getSequentialBrokers();
            if (brokers != null && !brokers.isEmpty()) {
                List<SequentialBrokerState.SequentialBroker> removedBrokers = new ArrayList<SequentialBrokerState
                                        .SequentialBroker>();
                for (SequentialBrokerState.SequentialBroker broker : brokers) {
                    if (!groups.contains(broker.getGroup())) {
                        removedBrokers.add(broker);
                    }
                }
                return topicState.removeBrokers(removedBrokers);
            }
        }

        return false;
    }


    /**
     * 判断主题是否可写
     *
     * @param topic  主题
     * @param broker broker
     * @return
     * @throws JMQException
     */
    public boolean isWritable(String topic, Broker broker) throws JMQException {
        SequentialTopicState topicState = topicStates.get(topic);
        boolean writable = false;
        if (topicState != null) {
            SequentialBrokerState brokerState = topicState.getBrokerState();
            if (brokerState != null) {
                SequentialBrokerState.SequentialBroker sequentialBroker = brokerState.fetchWritableBroker();
                String writableGroup = sequentialBroker != null ? sequentialBroker.getGroup() : null;
                // 可写的broker不为空，并且可写的broker上broker名称不为空 且当前broker的名称和可写的broker名称一致
                if (writableGroup != null && writableGroup.equals(broker.getGroup())) {
                    writable = true;
                }
            }
        }

        return writable;
    }


    /**
     * 检查app是否具有读权限
     *
     * @param topic 主题
     * @param app   应用
     * @return
     */
    public boolean checkReadable(String topic, String app) {
        SequentialTopicState topicState = topicStates.get(topic);
        return topicState.checkReadable(getCurrentGroupName(), app);
    }

    /**
     * 获取具有读权限的broker
     *
     * @param topic 主题
     * @param app   应用
     * @return
     */
    public SequentialBrokerState.SequentialBroker getReadableBroker(String topic, String app) {
        SequentialTopicState topicState = getAndCreateTopicState(topic);
        AppConsumeState consumeState = topicState.fetchAndCreateConsumeState(app);
        if (consumeState != null) {
            return consumeState.fetchReadableBroker();
        }
        return null;
    }

    /**
     * 检查顺序消息是否需要重新分配可用的broker 并移除不可写的broker的写权限
     *
     * @param clusters 集群列表
     * @param app      应用
     */
    public void checkSequentialCluster(final List<BrokerCluster> clusters, String app) {
        if (clusters == null || clusters.isEmpty() || app == null || app.isEmpty()) {
            return;
        }
        for (BrokerCluster brokerCluster : clusters) {
            String topic = brokerCluster.getTopic();
            TopicConfig topicConfig = clusterManager.getTopicConfig(topic);
            if (topicConfig != null && topicConfig.checkSequential()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Start check sequential topic state!");
                }
                SequentialTopicState topicState = getTopicStateFromRegistry(topic);
                if (topicState == null) {
                    topicState = new SequentialTopicState(topic);
                }

                getWriteLock().lock();
                try {
                    BrokerGroup reassignedGroup = null;
                    //拷贝一份
                    SequentialBrokerState brokerState = topicState.getBrokerState();
                    SequentialBrokerState.SequentialBroker sequentialBroker = brokerState.fetchWritableBroker();
                    List<BrokerGroup> groups = brokerCluster.getGroups();
                    boolean needSync = false;
                    if (sequentialBroker == null) {// 初次分配
                        for (BrokerGroup group : groups) {
                            if (isGroupAvailable(group)) {
                                reassignedGroup = group;
                                needSync = true;
                                break;
                            }
                        }
                    } else {// 非初次分配,检查是否需要重新分配
                        //当前可写的broker
                        String writableGroup = sequentialBroker.getGroup();
                        for (BrokerGroup group : groups) {
                            String groupName = group.getGroup();
                            if (writableGroup != null && writableGroup.equals(groupName)) { // 如果当前分配的broker还存活，则不进行重分配
                                if (isGroupAvailable(group)) {
                                    reassignedGroup = null;
                                    break;
                                }
                            } else if (reassignedGroup == null && brokerState.isWriteCandidate(groupName)) {
                                reassignedGroup = group;
                            }
                        }

                        if (reassignedGroup != null) {
                            // 检查重新分配的Broker 是否可用，如果不可用再尝试重新分配
                            if (!isGroupAvailable(reassignedGroup)) {
                                reassignedGroup = null;
                                for (BrokerGroup group : groups) {
                                    String groupName = group.getGroup();
                                    if (!groupName.equals(writableGroup)) {
                                        if (brokerState.isWriteCandidate(groupName) && isGroupAvailable(group)) {
                                            reassignedGroup = group;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if (reassignedGroup != null) {
                        logger.info(String.format("Sequential topic reassigned broker! topic=%s,broker=%s", topic, reassignedGroup));
                        //将重新分配的Broker添加到发送和消费列表
                        SequentialBrokerState.SequentialBroker candidate = new
                                SequentialBrokerState.SequentialBroker(reassignedGroup.getGroup());
                        brokerState.addWritableBroker(candidate);
                        needSync = true;
                    }

                    Map<String, TopicConfig.ConsumerPolicy> consumers = topicConfig.getConsumers();
                    if (consumers != null) {
                        //同步消费者数据
                        for (String key : consumers.keySet()) {
                            AppConsumeState consumeState = topicState.fetchConsumeState(key);
                            if (consumeState == null) {
                                // 获取并创建订阅信息
                                consumeState = topicState.fetchAndCreateConsumeState(key);
                                needSync = true;
                            }

                            // 添加可写的Broker
                            if (reassignedGroup != null) {
                                SequentialBrokerState.SequentialBroker writableBroker = new
                                        SequentialBrokerState.SequentialBroker(reassignedGroup.getGroup());
                                consumeState.addWritableBroker(writableBroker);
                                needSync = true;
                            }
                        }
                    }

                    // 移除已经取消订阅的消费者信息
                    ConcurrentMap<String, AppConsumeState> consumeStates = topicState.getAppConsumeStates();
                    if (consumeStates != null) {
                        if (consumers == null || consumers.isEmpty()) {
                            consumeStates.clear();
                            needSync = true;
                        } else {
                            for (String appKey : consumeStates.keySet()) {
                                if (!consumers.containsKey(appKey)) {
                                    consumeStates.remove(appKey);
                                    needSync = true;
                                }
                            }
                        }
                    }


                    if (needSync) {  //分组信息,消费信息如果变化，需要同步到zk
                        // 同步发送者数据
                        syncTopicState(topicState, topic, topicState.getVersion());
                    }
                } catch (Exception e) {
                    logger.warn("Reassigned sequential broker error!", e);
                } finally {
                    getWriteLock().unlock();
                }

                SequentialBrokerState brokerState = topicState.getBrokerState();
                SequentialBrokerState.SequentialBroker sequentialBroker = brokerState.fetchWritableBroker();
                String writableGroup = sequentialBroker != null ? sequentialBroker.getGroup() : null;
                //获取消费者列表
                SequentialBrokerState.SequentialBroker readableBroker = topicState.fetchReadableBroker(app);

                for (BrokerGroup group : brokerCluster.getGroups()) {
                    String groupName = group.getGroup();
                    //不可写则移除写权限
                    if (writableGroup == null || groupName == null || !groupName.equals(writableGroup)) {
                        group.setPermission(group.getPermission().removeWrite());
                    }
                    //不可读则移除读权限
                    if (readableBroker == null || readableBroker.getGroup() == null || groupName == null ||
                            !readableBroker.getGroup().equals(groupName)) {
                        group.setPermission(group.getPermission().removeRead());
                    }
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Finished check sequential topic state!");
                }
            }
        }

    }

    // 从注册中心获取顺序主题状态
    private SequentialTopicState getTopicStateFromRegistry(String topic) {
        SequentialTopicState topicState = null;
        if (topic != null) {
            try {
                String topicPath = Path.concat(config.getSequentialPath(), topic);
                PathData pathData = registry.getData(topicPath);
                if (pathData != null && pathData.getData() != null && pathData.getData().length != 0) {
                    String content = new String(pathData.getData(), Charset.forName("UTF-8"));
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("New sequential topicState=%s", content));
                    }
                    topicState = JSON.parseObject(content, SequentialTopicState.class);
                    if (topicState != null) {
                        topicState.setVersion(pathData.getVersion());
                    }
                }
            } catch (Exception e) {
                logger.error(String.format("Get topic state from registry error!topic=%s", topic), e);
            }
        }

        return topicState;
    }

    // 检测分组是否可用.至少是一主一从的情况下才是可用的
    private boolean isGroupAvailable(BrokerGroup group) {
        logger.info("Start check group state!");
        Boolean available = Boolean.FALSE;
        //TODO  检测分组是否可用
        logger.info(String.format("Check result, group=%s, available=%s", group.getGroup(), available));
        return available;
    }

    /**
     * 顺序消息绑定生产者
     *
     * @param app        应用
     * @param topic      主题
     * @param producerId 生产者ID
     * @throws JMQException
     */
    public void checkAndBindProducer(String app, String topic, String producerId) throws JMQException {
        boolean writable = false;
        if (app == null || topic == null || producerId == null || topic.isEmpty() || app.isEmpty() || producerId.isEmpty()) {
            writable = false;
        } else {
            ConcurrentMap<String, ConcurrentMap<String, String>> sequentialProducer = this.sequentialProducer;
            ConcurrentMap<String, String> appProducers = sequentialProducer.get(topic);
            if (appProducers == null) {
                appProducers = new ConcurrentHashMap<String, String>(100);
                ConcurrentMap<String, String> old = sequentialProducer.putIfAbsent(topic, appProducers);
                if (old != null) {
                    appProducers = old;
                }
            }
            String writableProducer = appProducers.get(app);
            if (writableProducer == null) {
                writableProducer = producerId;
                String old = appProducers.putIfAbsent(app, writableProducer);
                if (old != null) {
                    writableProducer = old;
                }
                logger.info(String.format("try to bind producer to sequential topic! topic=%s,oldProducer=%s, " +
                        "currentProducer=%s", topic, old, writableProducer));
            }

            writable = writableProducer.equals(producerId);
        }

        if (!writable) {
            //顺序消息，当前broker不可写
            logger.error(String.format("Sequential topic %s can not send message to this broker", topic));
            throw new JMQException(String.format("Sequential topic %s can not send message to this broker", topic), JMQCode.CN_NO_PERMISSION.getCode());
        }
    }

    /**
     * 解除顺序消息生产者绑定关系
     *
     * @param producer
     */
    public void unbindProducer(final com.ipd.jmq.common.network.v3.session.Producer producer) {
        ConcurrentMap<String, ConcurrentMap<String, String>> sequentialProducer = this.sequentialProducer;
        ConcurrentMap<String, String> producers = sequentialProducer.get(producer.getTopic());
        if (producers != null && !producers.isEmpty()) {
            String producerId = producers.get(producer.getApp());
            if (producerId != null && producerId.equals(producer.getId())) {
                producers.remove(producer.getApp());
            }
        }
    }

    /**
     * 监听注册中心主题变化
     */
    protected class SequentialTopicListener implements PathListener {
        @Override
        public void onEvent(PathEvent pathEvent) {
            getWriteLock().lock();
            try {
                if (isStarted()) {
                    try {
                        if (pathEvent != null) {
                            String topic = Path.node(pathEvent.getPath());
                            if (topic != null) {
                                updateTopicState(clusterManager.getTopicConfig(topic), pathEvent.getData(),
                                        pathEvent.getVersion());
                            }
                        }
                    } catch (Exception e) {
                        logger.error(String.format("Update produce state error, path: %s.", pathEvent.getPath()), e);
                    }
                }
            } finally {
                getWriteLock().unlock();
            }
        }
    }


    /**
     * 心跳任务
     */
    protected class SequentialStateTask extends ServiceThread {

        public SequentialStateTask(Service parent) {
            super(parent);
        }

        @Override
        protected void execute() throws Exception {
            //
            sinkBindProducer();

            Broker broker = clusterManager.getBroker();
            if (broker != null && ClusterRole.MASTER.equals(broker.getRole())) {
                flushTopicState();
            }
        }

        @Override
        public long getInterval() {
            return updateSequentialTopicInterval;
        }

        @Override
        public boolean onException(Throwable e) {
            logger.error(e.getMessage(), e);
            return true;
        }

        //解除已经不存在的生产者的绑定关系
        protected void sinkBindProducer() {
            ConcurrentMap<String, ConcurrentMap<String, String>> sequentialProducer = SequentialManager.this.sequentialProducer;
            if (sequentialProducer != null) {
                for (ConcurrentMap<String, String> producers : sequentialProducer.values()) {
                    for (Map.Entry<String, String> entry : producers.entrySet()) {
                        if (sessionManager.getProducerById(entry.getValue()) == null) {
                            producers.remove(entry.getKey());
                        }
                    }

                }
            }
        }

        // 刷新顺序消息状态
        protected void flushTopicState() throws JMQException {
            try {
                getWriteLock().lock();
                try {
                    logger.info("Start to check and flush sequential topic state to registry!");
                    ConcurrentMap<String, SequentialTopicState> topicStates = getTopicStates();

                    for (Map.Entry<String, SequentialTopicState> entry : topicStates.entrySet()) {
                        String topic = entry.getKey();
                        SequentialTopicState topicState = entry.getValue();
                        SequentialTopicState copyOfTopicState = topicState.clone();
                        if (!hasPriFlushTopicState(copyOfTopicState)) {
                            if (logger.isDebugEnabled()) {
                                logger.debug(String.format("No privilege to flush topic=%s", topic));
                            }
                            continue;
                        }

                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("Start check and flush! topic=%s", topic));
                        }

                        boolean needSync = false;
                        for (Map.Entry<String, AppConsumeState> appEntry : copyOfTopicState.getAppConsumeStates()
                                .entrySet()) {
                            try {
                                String app = appEntry.getKey();
                                AppConsumeState consumeState = appEntry.getValue();
                                if (updateConsumeState(topic, app, consumeState)) {
                                    needSync = true;
                                }
                            } catch (Exception e) {
                                logger.error("Update consumer state error!", e);
                            }
                        }

                        //如果当前节点是
                        SequentialBrokerState brokerState = copyOfTopicState.getBrokerState();
                        if (brokerState != null && getCurrentGroupName() != null) {
                            //如果当前broker是可写的broker（在此这个broker相当于/jmq/sequential/topic/app中的leader），遍历当前已分配的Broker
                            //将没有积压的broker从消费列表移除
                            String writeGroup = null;
                            SequentialBrokerState.SequentialBroker writableBroker = brokerState.fetchWritableBroker();
                            if (writableBroker != null) {
                                writeGroup = writableBroker.getGroup();
                            }

                            //TODO 如果不加此判断，此处会不会有问题
                            // if (writeGroup != null && localGroup.equals(writeGroup)) {
                            // 检查BROKER上的topic是否有积压（当有一个消费者有积压的时候就算积压）
                            //更新zk节点
                            if (logger.isDebugEnabled()) {
                                logger.debug(String.format("start sink sequential brokers, state[%s]", copyOfTopicState));
                            }
                            if (copyOfTopicState.sinkSequentialBrokers()) {
                                needSync = true;
                            }
                            if (logger.isDebugEnabled()) {
                                logger.debug(String.format("finished sink sequential brokers, state[%s]", copyOfTopicState));
                            }
                            // }
                        }


                        if (needSync) {
                            syncAndResetTopicState(copyOfTopicState);
                        }
                    }
                    logger.info("Finished check and flush sequential topic state to registry!");
                } finally {
                    getWriteLock().unlock();
                }
            } catch (Exception e) {
                throw new JMQException(JMQCode.FW_FLUSH_SEQUENTIAL_STATE_ERROR);
            }
        }


        //是否有权限更新主题信息
        protected boolean hasPriFlushTopicState(SequentialTopicState topicState) {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Valid flush privilege,current state:[%s]", topicState));
            }

            BrokerGroup brokerGroup = clusterManager.getBrokerGroup();
            String group = brokerGroup != null ? brokerGroup.getGroup() : null;
            SequentialBrokerState.SequentialBroker sequentialBroker = new SequentialBrokerState.SequentialBroker(group);
            SequentialBrokerState brokerState = topicState.getBrokerState();
            Deque<SequentialBrokerState.SequentialBroker> produceBrokers = brokerState.getSequentialBrokers();

            return produceBrokers.contains(sequentialBroker);
        }


    }

    private boolean updateConsumeState(String topic, String app, final AppConsumeState consumeState) {
        boolean changed = false;
        try {
            if (topic != null && !topic.isEmpty() && app != null && !app.isEmpty() && consumeState != null &&
                    dispatchService != null) {
                //更新积压信息
                changed = consumeState.updateState(dispatchService.isPendingExcludeRetry(topic, app),
                        getCurrentGroupName());
            }
        } catch (Exception e) {
            logger.warn(String.format("update or sync consume state error!topic=%s, app=%s", topic, app), e);
        }
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Update consume state, changed=%s", changed));
        }
        return changed;
    }

    protected class DispatchListener implements EventListener<DispatchEvent> {
        @Override
        public void onEvent(DispatchEvent event) {
            try {
                if (event != null && event.getType().equals(DispatchEvent.EventType.FREE)) {
                    String topic = event.getTopic();
                    String app = event.getApp();
                    SequentialTopicState topicState = topicStates.get(topic);
                    if (topicState != null) {
                        SequentialTopicState copyOfTopicState = topicState.clone();
                        final AppConsumeState consumeState = copyOfTopicState.fetchAndCreateConsumeState(app);
                        if (updateConsumeState(topic, app, consumeState)) {
                            logger.info(String.format("Consume state is changed try to sync and reset!topic=%s, app=%s", topic, app));
                            syncAndResetTopicState(copyOfTopicState);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error(String.format("Sync consume state error! event:%s", event), e);
            }
        }
    }

    private void syncAndResetTopicState(final SequentialTopicState topicState) {
        if (topicState == null) {
            return;
        }
        try {
            logger.info(String.format("Sequential topic state changed, will sync and reset! topic=%s, expectVersion=%s",
                    topicState.getTopic(), topicState.getVersion()));
            syncTopicState(topicState, topicState.getTopic(), topicState.getVersion());
            resetTopicState(topicState);
            logger.info("Sync and reset sequential topic state succeed! ");
        } catch (Exception e) {
            logger.error("Sync sequential topic state error!", e);
        }
    }

    private void syncTopicState(final SequentialTopicState topicState, String topic, int expectedVersion)
            throws Exception {

        if (topic == null || topic.isEmpty() || topicState == null) {
            return;
        }

        String json = JSON.toJSONString(topicState, SerializerFeature.DisableCircularReferenceDetect);
        String topicPath = Path.concat(config.getSequentialPath(), topic);
        byte[] data = json.getBytes(Charsets.UTF_8);
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Will sync topic state,topic=%s, content=[%s] to %s:%s, " +
                        "expectedVersion=%s", topic, json, topicPath, registry.getUrl(), expectedVersion));
            }

            registry.update(new PathData(topicPath, data, expectedVersion));
            topicState.setVersion(++expectedVersion);

            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Sync topic state success,topic=%s,data.length=%s", topic, data == null ? 0 : data.length));
            }
        } catch (Exception e) {
            if (e instanceof RegistryException.BadVersionException) {
                logger.info("Sync topic state failure, will attempt to get and update topic state!");
                PathData pathData = registry.getData(Path.concat(config.getSequentialPath(), topic));
                if (pathData != null) {
                    byte[] old = pathData.getData();
                    int version = pathData.getVersion();
                    if (old != null && old.length > 0) {
                        String content = new String(old, Charset.forName("UTF-8"));

                        if (content != null) {
                            SequentialTopicState newState = JSON.parseObject(content, SequentialTopicState.class);
                            if (newState != null) {
                                topicState.setAppConsumeStates(newState.getAppConsumeStates());
                                topicState.setBrokerState(newState.getBrokerState());
                            }
                        }
                        //更新版本号
                        topicState.setVersion(version);
                    } else if (version == 0) {
                        //在集群变更过程中节点的版本会发生改变，如果版本为0且无数据，则将当前数据同步到节点上
                        expectedVersion = version;
                        registry.update(new PathData(topicPath, data, expectedVersion));
                        topicState.setVersion(++expectedVersion);
                    }
                }
                logger.info("Get and update topic state successfully!");
            } else {
                throw e;
            }
        }
    }


    private void resetTopicState(final SequentialTopicState topicState) {
        if (topicState == null) {
            return;
        }
        logger.info(String.format("try to reset topic state,topic=%s,version=%s", topicState.getTopic(), topicState
                .getVersion()));
        synchronized (this) {
            SequentialTopicState old = topicStates.get(topicState.getTopic());
            if (old == null || old.getVersion() <= topicState.getVersion()) {
                topicStates.put(topicState.getTopic(), topicState);
            }
        }
    }

    protected SequentialTopicState getAndCreateTopicState(String topic) {
        if (topic == null) {
            throw new NullPointerException("topic can not be null");
        }
        SequentialTopicState topicState = topicStates.get(topic);
        if (topicState == null) {
            topicState = new SequentialTopicState(topic);
            SequentialTopicState old = topicStates.putIfAbsent(topic, topicState);
            if (old != null) {
                topicState = old;
            }
        }

        return topicState;
    }

    private String getCurrentGroupName() {
        Broker broker = clusterManager.getBroker();
        String group = broker != null ? broker.getGroup() : null;
        return group;
    }


    //----setter/getter----

    public Registry getRegistry() {
        return registry;
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    public DispatchService getDispatchService() {
        return dispatchService;
    }

    public void setDispatchService(DispatchService dispatchService) {
        this.dispatchService = dispatchService;
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    public long getUpdateSequentialTopicInterval() {
        return updateSequentialTopicInterval;
    }

    public void setUpdateSequentialTopicInterval(long updateSequentialTopicInterval) {
        this.updateSequentialTopicInterval = updateSequentialTopicInterval;
    }

    public Thread getUpdateSequentialThread() {
        return updateSequentialThread;
    }

    public void setUpdateSequentialThread(Thread updateSequentialThread) {
        this.updateSequentialThread = updateSequentialThread;
    }

    public long getJmxConnectionTimeout() {
        return jmxConnectionTimeout;
    }

    public void setJmxConnectionTimeout(long jmxConnectionTimeout) {
        this.jmxConnectionTimeout = jmxConnectionTimeout;
    }

    public ConcurrentMap<String, SequentialTopicState> getTopicStates() {
        return topicStates;
    }

    public void setTopicStates(ConcurrentMap<String, SequentialTopicState> topicStates) {
        this.topicStates = topicStates;
    }

    public ClusterManager getClusterManager() {
        return clusterManager;
    }

    public void setClusterManager(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public BrokerConfig getConfig() {
        return config;
    }

    public void setConfig(BrokerConfig config) {
        this.config = config;
    }

    public BrokerMonitor getBrokerMonitor() {
        return brokerMonitor;
    }

    public void setBrokerMonitor(BrokerMonitor brokerMonitor) {
        this.brokerMonitor = brokerMonitor;
    }

}
