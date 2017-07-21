package com.ipd.jmq.server.broker.cluster;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.ipd.jmq.common.cluster.*;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.lb.ClientType;
import com.ipd.jmq.common.lb.TopicLoad;
import com.ipd.jmq.common.model.DynamicClientConfig;
import com.ipd.jmq.common.network.ServerConfig;
import com.ipd.jmq.registry.PathData;
import com.ipd.jmq.registry.Registry;
import com.ipd.jmq.registry.RegistryException;
import com.ipd.jmq.registry.listener.PathEvent;
import com.ipd.jmq.registry.listener.PathListener;
import com.ipd.jmq.registry.util.Path;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.controller.ControllerService;
import com.ipd.jmq.server.broker.monitor.BrokerMonitor;
import com.ipd.jmq.server.broker.retry.RetryConfig;
import com.ipd.jmq.server.broker.utils.BrokerUtils;
import com.ipd.jmq.server.store.StoreConfig;
import com.ipd.jmq.toolkit.concurrent.EventBus;
import com.ipd.jmq.toolkit.io.Compressors;
import com.ipd.jmq.toolkit.io.Zip;
import com.ipd.jmq.toolkit.network.Ipv4;
import com.ipd.jmq.toolkit.network.Lan;
import com.ipd.jmq.toolkit.retry.RetryPolicy;
import com.ipd.jmq.toolkit.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import  com.ipd.jmq.toolkit.concurrent.EventListener;

/**
 * 集群管理
 */
public class ClusterManager extends Service {
    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);
    public static final String BROKER_FILE_NAME = "brokers";
    public static final String TOPIC_FILE_NAME = "topics";
    public static final String DATA_CENTER_FILE_NAME = "datacenters";
    public static final String LOCAL_CONFIG_DIR = "configs";
    private static final int WEIGHT_TOTAL = 100;

    // 注册中心
    private Registry registry;
    // 配置
    private BrokerConfig config;
    // 本地Broker
    private Broker broker;
    // 当前分组
    private BrokerGroup brokerGroup;
    // Broker监听器
    private PathListener brokerListener = new BrokerListener();
    // 主题监听器
    private PathListener topicListener = new TopicListener();
    // 数据中心监听器
    private PathListener dataCenterListener = new DataCenterListener();
    // Group
    private Map<String, BrokerGroup> groups = new HashMap<String, BrokerGroup>();
    // Broker
    private Map<String, Broker> brokers = new HashMap<String, Broker>();
    // 集群配置信息
    private Map<String, TopicConfig> topics = new HashMap<String, TopicConfig>();
    // 集群
    private ConcurrentMap<String, BrokerCluster> clusters = new ConcurrentHashMap<String, BrokerCluster>();

    private EventBus<ClusterEvent> eventManager = new EventBus<ClusterEvent>("ClusterManager");
    // 数据中心
    private List<DataCenter> dataCenters = new ArrayList<DataCenter>();
    // 重试服务地址
    private Set<String> retryAddresses = new HashSet<String>();
    // 直连重试服务地址
    private Set<String> fixRetryAddresses = new HashSet<String>();
    // Broker 监控接口
    private BrokerMonitor brokerMonitor;
    // Broker配置信息文件
    private File brokerFile;
    // topic配置文件
    private File topicFile;
    // 数据中心配置文件
    private File dataCenterFile;
    // 本地配置路径
    private File localConfigDir;
    // 控制服务
    private ControllerService controllerService;
    // 切换成从消费的slave的map
    private Map<String, String> slaveConsumeMap = new ConcurrentHashMap<String, String>();
    // 顺序消息管理器
    private SequentialManager sequentialManager;
    // 负载均衡结果
//    private CacheService cacheService;

    private boolean loadBrokers = false;
    private boolean loadTopics = false;
    private boolean loadConfig = false;
    private boolean loadDatacenter = false;

    public ClusterManager(BrokerConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config can not be null");
        }

        this.config = config;
        this.registry = config.getRegistry();

        // 构造本地Broker
        ServerConfig serverConfig = config.getServerConfig();
        int port = serverConfig.getPort();
        try {
            broker = new Broker("".equals(serverConfig.getIp())? Ipv4.getLocalIp():serverConfig.getIp(), port, null);
        } catch (SocketException e) {
            e.printStackTrace();
        }
        String group = config.getGroup();
        if (group != null && !group.isEmpty()) {
            broker.setGroup(group);
        }
        brokerGroup = new BrokerGroup();
        brokerGroup.addBroker(broker);
    }

    public SequentialManager getSequentialManager() {
        return sequentialManager;
    }

    public void setSequentialManager(SequentialManager sequentialManager) {
        this.sequentialManager = sequentialManager;
    }

    /**
     * 设置生产者的权重。
     *
     * @param cluster 集群
     * @param policy  生产者策略
     */
    protected static void updateWeight(final BrokerCluster cluster, final TopicConfig.ProducerPolicy policy, long clientDataCenter) {
        if (cluster == null) {
            return;
        }
        List<BrokerGroup> groups = cluster.getGroups();
        // 得到分组权限
        Map<String, Short> weights = null;
        if (policy != null) {
            weights = policy.getWeight();
        }
        int totalWeight = groups.size() <= 100 ? WEIGHT_TOTAL : groups.size();
        int weightSum = 0;
        int noWeights = 0;

        Short weight;
        //用户指定权限
        boolean userSetWeight = false;
        // 遍历分组
        for (BrokerGroup group : groups) {
            // 默认设置为未分配
            group.setWeight((short) -1);
            // 获取当前分组权重
            weight = null;
            if (weights != null) {
                weight = weights.get(group.getGroup());
            }
            if (weight != null) {
                // 配置了有效的权重
                if (weight >= 0) {
                    userSetWeight = true;
                    group.setWeight(weight);
                    weightSum += weight;
                } else {
                    // 未分配权限
                    noWeights++;
                }
            } else {
                // 未分配权重
                noWeights++;
            }
        }

        // 有未分配权重
        if (noWeights > 0) {
            // 计算剩余权重
            short remain = (short) (totalWeight - weightSum);
            // 未分配的平均权重
            weight = (short) (remain / noWeights);
            if (weight < 0) {
                weight = 0;
            } else if (weight == 0 && remain > 0) {
                // 如果分组太多，剩余计算可能为0
                weight = 1;
            }


            List<BrokerGroup> nearGroups = nearByGroups(clientDataCenter, groups);
            if (!userSetWeight && policy != null && policy.isNearby() && nearGroups.size() > 0) {
                /**
                 * 用户未配置消费权限，并且开启了就近消费
                 */
                //非就近生产默认权重为1
                short notNearByWeight = 1;
                //就近生产权重 ，去除非就近剩余就近的group平分权重, 分组数>100 时所有的
                int notNearByGroupSize = groups.size() - nearGroups.size();
                short nearByWeight = (short) ((totalWeight - (notNearByGroupSize)) / nearGroups.size());

                for (BrokerGroup group : groups) {
                    if (nearGroups.contains(group)) {
                        group.setWeight(nearByWeight);
                    } else {
                        //非同一机房，最小化权重
                        group.setWeight(notNearByWeight);
                    }
                }

            } else {
                // 遍历分组，设置权重
                for (BrokerGroup group : groups) {
                    if (group.getWeight() != -1) {
                        continue;
                    }

                    noWeights--;
                    if (noWeights == 0) {
                        // 最后一个，权重应该为剩余
                        group.setWeight(remain > 0 ? remain : 0);
                        break;
                    }

                    if (remain >= weight) {
                        group.setWeight(weight);
                    } else if (remain > 0) {
                        group.setWeight(remain);
                    } else {
                        group.setWeight((short) 0);
                    }

                    remain -= weight;

                }
            }
        }
    }

    private static List<BrokerGroup> nearByGroups(long clientDataCenter, List<BrokerGroup> allGroups) {

        List<BrokerGroup> result = new ArrayList<BrokerGroup>();

        for (BrokerGroup brokerGroup : allGroups) {
            for (Broker broker : brokerGroup.getBrokers()) {
                if (broker.getPermission().canWrite() && broker.getDataCenter() == clientDataCenter) {
                    result.add(brokerGroup);
                    break;
                }
            }
        }

        return result;

    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();

        controllerService = new ControllerService(config.getTopicControllerPolicyMap(), config.getAppControllerPolicyMap());

        // 设置直连重试地址
        RetryConfig retryConfig = config.getRetryConfig();
        fixRetryAddresses = retryConfig.getFixAddresses();

        eventManager.start();
        eventManager.addListener(new EventListener<ClusterEvent>() {
            @Override
            public void onEvent(ClusterEvent event) {
                if (event == null || !(event instanceof TopicUpdateEvent)) {
                    return;
                }
                TopicConfig topicConfig = ((TopicUpdateEvent) event).getTopicConfig();
                if (topicConfig != null) {
                    sequentialManager.updateTopicStateAndAddListener(topicConfig);
                }
            }
        });
        String livePath = config.getLivePath();
        if (livePath != null && !livePath.isEmpty()) {
            registry.createLive(Path.concat(livePath, broker.getName()), null);
        }
        if (registry.isConnected()) {
            // 初始化
            updateBroker();
            updateTopic();
            updateDateCenter();
        }

        if (config.isUseLocalConfig()) {
            //使用本地配置初始化
            if (!loadBrokers) {
                loadLocalBrokers();
            }
            if (!loadTopics) {
                loadLocalTopics();
            }
            if (!loadDatacenter) {
                loadLocalDataCenter();
            }
        }

        registry.addListener(config.getBrokerPath(), brokerListener);
        registry.addListener(config.getTopicPath(), topicListener);
        registry.addListener(config.getDataCenterPath(), dataCenterListener);

//        if (null == cacheService) {
//            cacheService = config.getCacheService();
//        }

        logger.info("cluster manager is started");
    }

    /**
     * 添加基础信息监控
     *
     * @param fetchRegistry
     */
    public void addMetaConfigListener(Registry fetchRegistry) {
        fetchRegistry.addListener(config.getBrokerPath(), brokerListener);
        fetchRegistry.addListener(config.getTopicPath(), topicListener);
        fetchRegistry.addListener(config.getDataCenterPath(), dataCenterListener);
    }

    public void updateCluster() {
        if (registry.isConnected()) {
            // 初始化
            updateBroker();
            updateTopic();
            updateDateCenter();
        }
    }

    private void loadLocalBrokers() {
        String content = null;
        try {
            brokerFile = new File(getAndCreateConfigDir(), BROKER_FILE_NAME);
            content = (String) BrokerUtils.readConfigFile(brokerFile, String.class, "");
            List<Broker> brokers = JSON.parseArray(content, Broker.class);
            if (brokers != null && !brokers.isEmpty()) {
                updateBroker(brokers);
            }
        } catch (Exception e) {
            logger.error(String.format("Init local brokers error! content:%s", content), e);
        }
    }

    private void loadLocalTopics() {
        String content = null;
        try {
            topicFile = new File(getAndCreateConfigDir(), TOPIC_FILE_NAME);
            content = (String) BrokerUtils.readConfigFile(topicFile, String.class, "");
            List<TopicConfig> configs = JSON.parseArray(content, TopicConfig.class);
            if (configs != null && !configs.isEmpty()) {
                updateTopic(configs);
            }
        } catch (Exception e) {
            logger.error(String.format("Init local topics error! content:%s", content), e);
        }
    }

    private void loadLocalDataCenter() {
        String content = null;
        try {
            dataCenterFile = new File(getAndCreateConfigDir(), DATA_CENTER_FILE_NAME);
            content = (String) BrokerUtils.readConfigFile(dataCenterFile, String.class, "");
            if (content != null && !content.isEmpty()) {
                this.dataCenters = JSON.parseArray(content, DataCenter.class);
            }
        } catch (Exception e) {
            logger.error(String.format("Init local data center error! content:%s", content), e);
        }
    }

    public File getAndCreateConfigDir() {
        if (localConfigDir == null) {
            try {
                File configDir = null;
                StoreConfig storeConfig = config.getStoreConfig();
                File dataDir = null;
                if (storeConfig != null) {
                    dataDir = storeConfig.getDataDirectory();
                }

                if (dataDir != null) {
                    configDir = new File(dataDir, LOCAL_CONFIG_DIR);
                    configDir.mkdirs();
                }
                localConfigDir = configDir;
            } catch (Exception e) {
                logger.error("create local config dir error!", e);
            }
        }
        return localConfigDir;
    }

    private void saveBrokers(String content) {
        try {
            if (content == null || content.isEmpty()) {
                return;
            }
            if (brokerFile == null) {
                brokerFile = new File(getAndCreateConfigDir(), BROKER_FILE_NAME);
            }
            BrokerUtils.writeConfigFile(brokerFile, content);
        } catch (Throwable th) {
            logger.error("Save local brokers error!", th);
        }
    }

    private void saveTopics(String content) {
        try {
            if (content == null || content.isEmpty()) {
                return;
            }
            if (topicFile == null) {
                topicFile = new File(getAndCreateConfigDir(), TOPIC_FILE_NAME);
            }
            BrokerUtils.writeConfigFile(topicFile, content);
        } catch (Throwable th) {
            logger.error("Save local topics error!", th);
        }
    }

    private void saveDataCenter(String content) {
        try {
            if (content == null || content.isEmpty()) {
                return;
            }
            if (dataCenterFile == null) {
                dataCenterFile = new File(getAndCreateConfigDir(), DATA_CENTER_FILE_NAME);
            }
            BrokerUtils.writeConfigFile(dataCenterFile, content);
        } catch (Throwable th) {
            logger.error("Save local data center error!", th);
        }
    }

    @Override
    protected void doStop() {
        super.doStop();
        String livePath = config.getLivePath();
        if (livePath != null && !livePath.isEmpty()) {
            try {
                registry.deleteLive(Path.concat(livePath, broker.getName()));
            } catch (Exception ignored) {
            }
        }
        registry.removeListener(config.getBrokerPath(), brokerListener);
        registry.removeListener(config.getTopicPath(), topicListener);
        registry.removeListener(config.getDataCenterPath(), dataCenterListener);
        eventManager.stop();
        logger.info("cluster manager is stopped");
    }

    public List<DataCenter> getDataCenters() {
        return dataCenters;
    }

    public Map<String, TopicConfig> getTopics() {
        return topics;
    }


    public BrokerMonitor getBrokerMonitor() {
        return brokerMonitor;
    }

    public void setBrokerMonitor(BrokerMonitor brokerMonitor) {
        this.brokerMonitor = brokerMonitor;
    }

    /**
     * 获取数据中心
     *
     * @param clientIp 客户端IP
     * @return 数据中心
     */
    public DataCenter getDataCenter(String clientIp) {
        if (clientIp == null || clientIp.isEmpty()) {
            return null;
        }
        List<DataCenter> dataCenters = getDataCenters();
        if (dataCenters != null && !dataCenters.isEmpty()) {
            for (DataCenter dataCenter : dataCenters) {
                Lan lan = new Lan("");
//                if (dataCenter.match(clientIp)) {
//                    return dataCenter;
//                }
                if (lan.contains(clientIp)){
                    return dataCenter;
                }
            }
        }
        return null;
    }

    /**
     * 检查是否能生产数据
     *
     * @param topic 主题
     * @param app   应用
     * @return 主题配置
     * @throws JMQException
     */
    public TopicConfig checkWritable(final String topic, final String app) throws
            JMQException {
        if (!broker.isWritable()) {
            throw new JMQException(broker.getName() + " is not writable", JMQCode.CN_NO_PERMISSION.getCode());
        }

        // 禁用了
        TopicConfig topicConfig = getTopicConfig(topic);
        if (topicConfig == null) {
            // 没有分配该主题给应用
            throw new JMQException(String.format("%s is not registered", topic), JMQCode.CN_NO_PERMISSION.getCode());
        }
        if (!topicConfig.getGroups().contains(broker.getGroup())) {
            // 没有分配该主题给应用
            throw new JMQException(String.format("%s is not assigned to %s", broker.getName(), topic),
                    JMQCode.CN_NO_PERMISSION.getCode());
        }

        TopicConfig.ProducerPolicy policy = topicConfig.getProducerPolicy(app);
        if (policy == null) {
            throw new JMQException(String.format("%s is not registered to %s", app, topic),
                    JMQCode.CN_NO_PERMISSION.getCode());
        }

        if (topicConfig.checkSequential()) {
            if (!sequentialManager.isWritable(topic, broker)) {
                //顺序消息，当前broker不可写
                throw new JMQException(String.format("Sequential topic %s can not send message to %s", topic, broker.getGroup()), JMQCode.CN_NO_PERMISSION.getCode());
            }
        }
        return topicConfig;
    }

    /**
     * 检查是否能消费数据
     *
     * @param topic 主题
     * @param app   应用
     * @return 消费策略
     * @throws JMQException
     */
    public TopicConfig.ConsumerPolicy checkReadable(final String topic, final String app) throws JMQException {
        if (!broker.isReadable()) {
            // 机器被禁用消费
            throw new JMQException(broker.getName() + " is not readable", JMQCode.CN_NO_PERMISSION.getCode());
        }

        if (config.isStandbyMode() && broker.getRole() != ClusterRole.MASTER) {
            // 从只是备机，不能消费
            throw new JMQException(broker.getName() + " is not readable", JMQCode.CN_NO_PERMISSION.getCode());
        }

        // 权限判断
        TopicConfig topicConfig = getTopicConfig(topic);
        if (topicConfig == null) {
            // 没有分配该主题给应用
            throw new JMQException(String.format("%s is not registered", topic), JMQCode.CN_NO_PERMISSION.getCode());
        }
        if (!topicConfig.getGroups().contains(broker.getGroup())) {
            // 没有分配该主题给应用
            throw new JMQException(String.format("%s is not assigned to %s", broker.getName(), topic),
                    JMQCode.CN_NO_PERMISSION.getCode());
        }
        TopicConfig.ConsumerPolicy policy = topicConfig.getConsumerPolicy(app);
        if (policy == null) {
            throw new JMQException(String.format("%s has not subscribed to %s", app, topic),
                    JMQCode.CN_NO_PERMISSION.getCode());
        }
        if (policy.isPaused()) {
            throw new JMQException(String.format("%s has stopped consuming %s", app, topic),
                    JMQCode.CN_NO_PERMISSION.getCode());
        }

        return policy;
    }

    /**
     * 检测是否通过控制限制该主题或应用的写权限
     *
     * @param topic
     * @param app
     * @param address
     * @return
     */
    public JMQCode checkControllerWritable(final String topic, final String app, final String address) {

        boolean isTopicBrokNotWritable = controllerService.topicBrokerPermission(topic, brokerGroup.getGroup(), true);
        if (isTopicBrokNotWritable) {
            return JMQCode.FW_PUT_MESSAGE_TOPIC_NOT_WRITE;
        }
        boolean isAppClientIPLimitWritable = controllerService.appClientIPLimit(topic, app, address, true);
        if (isAppClientIPLimitWritable) {
            return JMQCode.FW_PUT_MESSAGE_APP_CLIENT_IP_NOT_WRITE;
        }
        return JMQCode.SUCCESS;
    }

    /**
     * 检测是否通过控制限制该主题或应用的读权限
     *
     * @param topic
     * @param app
     * @param address
     * @return
     */
    public JMQCode checkControllerReadable(final String topic, final String app, final String address) {

        boolean isTopicBrokNotReadable = controllerService.topicBrokerPermission(topic, brokerGroup.getGroup(), false);
        if (isTopicBrokNotReadable) {
            return JMQCode.FW_GET_MESSAGE_TOPIC_NOT_READ;
        }
        boolean isAppClientIPLimitReadable = controllerService.appClientIPLimit(topic, app, address, false);
        if (isAppClientIPLimitReadable) {
            return JMQCode.FW_GET_MESSAGE_APP_CLIENT_IP_NOT_READ;
        }
        return JMQCode.SUCCESS;
    }

    public boolean checkSequentialReadable(String topic, String app) {
        return sequentialManager.checkReadable(topic, app);
    }

    /**
     * 是否需要长轮询
     *
     * @param topic 主题
     * @return 是否长轮询
     */
    public boolean isNeedLongPull(String topic) {
        if (topic == null) {
            return false;
        }

        // 顺序消息不支持长轮询
        TopicConfig config = topics.get(topic);
        return !(config != null && config.checkSequential());
    }

    /**
     * 更新主题集群信息
     *
     * @param configs 主题集群信息
     */
    protected void updateTopic(List<TopicConfig> configs) {
        if (configs == null) {
            return;
        }

        Map<String, TopicConfig> current = new HashMap<String, TopicConfig>(configs.size());
        for (TopicConfig config : configs) {
            //分配到了本实例
            if (!config.getGroups().isEmpty() && broker.getGroup() != null && !broker.getGroup().isEmpty() && config
                    .getGroups().contains(broker.getGroup())) {
                //1.通知给存储用于比较消费者是否发生变化,oldConfig is null 也可能时运行中添加了TOPIC
                //2.通知给顺序消息用于更新顺序消息信息
                eventManager.add(new TopicUpdateEvent(config));
            }

            current.put(config.getTopic(), config);
        }
        this.topics = current;
        this.clusters = new ConcurrentHashMap<String, BrokerCluster>();

        eventManager.add(new UpdateNotifyEvent(ClusterEvent.EventType.ALL_TOPIC_UPDATE));
    }

    /**
     * 更新主题
     */
    protected void updateTopic() {
        try {
            PathData pathData = registry.getData(config.getTopicPath());
            updateTopic(pathData.getData());
            loadTopics = true;
        } catch (RegistryException e) {
            logger.error(String.format("get data %s error.", config.getTopicPath()), e);
        } catch (IOException e) {
            logger.error(String.format("decompress %s error.", config.getTopicPath()), e);
        }
    }

    /**
     * 更新主题集群信息
     *
     * @param data 数据
     * @throws IOException
     */
    protected void updateTopic(byte[] data) throws IOException {
        if (data == null || data.length == 0) {
            return;
        }
        String content;
        if (config.isCompressed()) {
            content = new String(Compressors.decompress(data, Zip.INSTANCE));
        } else {
            content = new String(data, Charset.forName("UTF-8"));
        }
        if (logger.isInfoEnabled()) {
            logger.info("update topic config.");
        } else if (logger.isDebugEnabled()) {
            logger.debug("update topic config." + content);
        }
        try {
            List<TopicConfig> configs = JSON.parseArray(content, TopicConfig.class);
            updateTopic(configs);
            // 缓存topic到本地
            if (config.isUseLocalConfig()) {
                saveTopics(content);
            }
        } catch (Exception e) {
            logger.warn("update topic config cause error, the content:{}",content);
        }
    }

    /**
     * 更新集群信息
     */
    protected void updateBroker() {
        try {
            PathData pathData = registry.getData(config.getBrokerPath());
            updateBroker(pathData.getData());
            loadBrokers = true;
        } catch (RegistryException e) {
            logger.error(String.format("get data %s error.", config.getBrokerPath()), e);
        } catch (IOException e) {
            logger.error(String.format("decompress %s error.", config.getBrokerPath()), e);
        }
    }

    /**
     * 更新集群
     *
     * @param data 数据
     * @throws IOException
     */
    protected void updateBroker(byte[] data) throws IOException {
        if (data == null || data.length == 0) {
            return;
        }
        String content;
        if (config.isCompressed()) {
            content = new String(Compressors.decompress(data, Zip.INSTANCE));
        } else {
            content = new String(data, Charset.forName("UTF-8"));
        }
        if (logger.isInfoEnabled()) {
            logger.info("update broker.");
        } else if (logger.isDebugEnabled()) {
            logger.debug("update broker." + content);
        }
        try {
            List<Broker> brokers = JSON.parseArray(content, Broker.class);
            updateBroker(brokers);
            if (config.isUseLocalConfig()) {
                saveBrokers(content);
            }
        } catch (Exception e) {
            logger.warn("update broker config cause error, the content:{}",content);
        }
    }

    /**
     * 更新数据中心
     */
    protected void updateDateCenter() {
        try {
            PathData pathData = registry.getData(config.getDataCenterPath());
            updateDataCenter(pathData.getData());
            loadDatacenter = true;
        } catch (RegistryException e) {
            logger.error(String.format("get data %s error.", config.getDataCenterPath()), e);
        }
    }

    /**
     * 更新数据中心
     *
     * @param data 数据
     */
    protected void updateDataCenter(byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }
        String content = new String(data, Charset.forName("UTF-8"));
        if (logger.isInfoEnabled()) {
            logger.info("update data center.");
        } else if (logger.isDebugEnabled()) {
            logger.debug("update data center." + content);
        }
        this.dataCenters = JSON.parseArray(content, DataCenter.class);
        //缓存到本地
        if (config.isUseLocalConfig()) {
            saveDataCenter(content);
        }
    }

    /**
     * 更新Broker
     *
     * @param brokers Broker信息
     */
    protected void updateBroker(List<Broker> brokers) {
        if (brokers == null) {
            return;
        }
        Broker source = this.broker;
        Set<String> retryAddresses = new HashSet<String>();
        Map<String, Broker> brokerMap = new HashMap<String, Broker>(brokers.size());
        Map<String, BrokerGroup> groupMap = new HashMap<String, BrokerGroup>(brokers.size());
        BrokerGroup group;
        boolean isMatch = false;
        // 遍历Broker
        for (Broker broker : brokers) {
            // 存放当前Broker
            brokerMap.put(broker.getName(), broker);
            // 获取分组
            group = groupMap.get(broker.getGroup());
            if (group == null) {
                // 分组不存在，则创建
                group = new BrokerGroup();
                group.setGroup(broker.getGroup());
                groupMap.put(broker.getGroup(), group);
            }
            // 添加到分组中
            group.addBroker(broker);

            if (broker.equals(source)) {
                // 等于当前Broker
                if (broker.getGroup() != null && !slaveConsumeMap.containsKey(broker.getGroup())) {
                    source.setPermission(broker.getPermission());
                }
                source.setGroup(broker.getGroup());
                source.setAlias(broker.getAlias());
                source.setDataCenter(broker.getDataCenter());
                source.setSyncMode(broker.getSyncMode());
                source.setRetryType(broker.getRetryType());
                //broker的角色会随着选举发生变化（registry模式下），不能以db中的为准
                //source.setRole(broker.getRole());
                if (broker.getReplicationPort() > 0) {
                    source.setReplicationPort(broker.getReplicationPort());
                }
                this.brokerGroup = group;
                isMatch = true;
            }
            // 重试服务，过滤掉停用的服务
            if (broker.getRetryType() == RetryType.DB && broker.getPermission() != Permission.NONE) {
                retryAddresses.add(broker.getName());
            }
        }
        if (!isMatch) {
            logger.error(String.format("broker config maybe error,can not find %s", this.broker));
        }
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("source:%s,group:%s", source.toString(), this.brokerGroup.toString()));
        }
        this.brokers = brokerMap;
        this.groups = groupMap;
        this.clusters = new ConcurrentHashMap<String, BrokerCluster>();
        // 判断是否使用直连重试地址
        retryAddresses = fixRetryAddresses != null && !fixRetryAddresses.isEmpty() ? fixRetryAddresses : retryAddresses;
        // 重试服务地址发生变更
        if (!retryAddresses.equals(this.retryAddresses)) {
            this.retryAddresses = retryAddresses;
            eventManager.add(new RetryEvent(retryAddresses));
        }

        eventManager.add(new UpdateNotifyEvent(ClusterEvent.EventType.ALL_BROKER_UPDATE));
    }

    /**
     * 获取主题配置信息
     *
     * @param topic 主题
     * @return 主题配置
     */
    public TopicConfig getTopicConfig(String topic) {
        return topics.get(topic);
    }

    /**
     * 获取重试策略，没用配置则返回默认重试策略
     *
     * @param topic 主题
     * @param app   应用
     * @return 重试策略
     */
    public RetryPolicy getRetryPolicy(String topic, String app) {
        TopicConfig topicConfig = topics.get(topic);
        TopicConfig.ConsumerPolicy consumerPolicy = null;
        if (topicConfig != null) {
            consumerPolicy = topicConfig.getConsumerPolicy(app);
        }
        if (consumerPolicy == null || consumerPolicy.getRetryPolicy() == null) {
            return config.getRetryPolicy();
        }
        return consumerPolicy.getRetryPolicy();
    }

    /**
     * 获取应答超时时间
     *
     * @param topic 主题
     * @param app   应用
     * @return 应答超时时间
     */
    public int getAckTimeout(String topic, String app) {
        TopicConfig topicConfig = topics.get(topic);
        TopicConfig.ConsumerPolicy consumerPolicy = null;
        if (topicConfig != null) {
            consumerPolicy = topicConfig.getConsumerPolicy(app);
        }
        if (consumerPolicy == null || consumerPolicy.getAckTimeout() == null) {
            return config.getAckTimeout();
        }
        return consumerPolicy.getAckTimeout();
    }

    /**
     * 获取当前Broker
     *
     * @return 当前Broker
     */
    public Broker getBroker() {
        return broker;
    }

    /**
     * 获取配置
     *
     * @return 配置
     */
    public BrokerConfig getConfig() {
        return config;
    }

    /**
     * 获取注册中心
     *
     * @return 注册中心
     */
    public Registry getRegistry() {
        return registry;
    }

    /**
     * 获取当前集群分组
     *
     * @return 当前集群分组
     */
    public BrokerGroup getBrokerGroup() {
        return brokerGroup;
    }

    /**
     * 获取分组对象
     *
     * @param group 分组名称
     * @return 分组对象
     */
    public BrokerGroup getBrokerGroup(final String group) {
        if (groups.isEmpty()) {
            writeLock.lock();
            try {
                if (groups.isEmpty()) {
                    updateBroker();
                }
            } finally {
                writeLock.unlock();
            }
        }
        return groups.get(group);
    }

    public Map<String, Broker> getBrokers() {
        return brokers;
    }

    /**
     * 获取Broker信息
     *
     * @param name 名称
     * @return Broker
     */
    public Broker getBroker(final String name) {
        if (brokers.isEmpty()) {
            writeLock.lock();
            try {
                if (brokers.isEmpty()) {
                    updateBroker();
                }
            } finally {
                writeLock.unlock();
            }
        }
        return brokers.get(name);
    }

    /**
     * 获取集群
     *
     * @param topic 主题
     * @return 集群
     */
    public BrokerCluster getCluster(String topic) {
        ConcurrentMap<String, BrokerCluster> clusters = this.clusters;
        Map<String, TopicConfig> topics = this.topics;

        BrokerCluster cluster = clusters.get(topic);
        if (cluster == null) {
            //创建
            TopicConfig config = topics.get(topic);
            if (config != null) {
                cluster = new BrokerCluster();
                cluster.setTopic(topic);
                cluster.setQueues(config.getQueues());

                BrokerGroup group;
                for (String name : config.getGroups()) {
                    group = getBrokerGroup(name);
                    if (group != null) {
                        cluster.addGroup(group);
                    }
                }
                clusters.putIfAbsent(topic, cluster);
            }
        }

        return cluster;
    }


    /**
     * 获取集群
     *
     * @param app 应用
     * @return 集群列表
     */
    public List<BrokerCluster> getClusters(String app, long dataCenter, String clientId) {

        List<BrokerCluster> result = new ArrayList<BrokerCluster>();
        if (app == null || app.isEmpty()) {
            return result;
        }

        Map<String, List<String>> clientIdsOfTopic = new HashMap<String, List<String>>();

        for (TopicConfig topicConfig : topics.values()) {

            String topic = topicConfig.getTopic();
            Permission clusterPermission = Permission.NONE;
            BrokerCluster cluster = getCluster(topic);
            TopicConfig.ConsumerPolicy consumerPolicy = topicConfig.getConsumerPolicy(app);
            TopicConfig.ProducerPolicy producerPolicy = topicConfig.getProducerPolicy(app);
            // 不能消费也不能生产
            if (((consumerPolicy == null || consumerPolicy.isPaused()) && producerPolicy == null)
                    || cluster.getGroups().isEmpty()) {
                continue;
            }

            // 先克隆一份
            cluster = cluster.clone();

            List<String> allProducerClientIds = new ArrayList<String>();
            List<String> allConsumerClientIds = new ArrayList<String>();

            // 遍历分组，判断分组权限
            for (BrokerGroup brokerGroup : cluster.getGroups()) {
                Permission groupPermission = Permission.NONE;

               // if (null != cacheService) {
                    String topicLoadString = "";//cacheService.get(topic + "." + app);
                    if (null != topicLoadString && !topicLoadString.isEmpty()) {
                        TopicLoad topicLoad = JSON.parseObject(topicLoadString, TopicLoad.class);
                        for (String group : topicLoad.getLoads().keySet()) {
                            if (!group.equals(brokerGroup.getGroup())) {
                                continue;
                            }
                            List<String> allowedConsumers = topicLoad.getLoads().get(group).get(ClientType.CONSUMER);
                            List<String> allowedProducers = topicLoad.getLoads().get(group).get(ClientType.PRODUCER);
                            if (null != allowedConsumers && !allowedConsumers.isEmpty()) {
                                allConsumerClientIds.addAll(allowedConsumers);
                            }
                            if (null != allowedProducers && !allowedProducers.isEmpty()) {
                                allProducerClientIds.addAll(allowedProducers);
                            }
                        }
                    }
                //}

                // 遍历Broker，判断权限
                for (Broker broker : brokerGroup.getBrokers()) {
                    Permission brokerPermission = getPermission(broker, dataCenter, consumerPolicy, producerPolicy);
                    broker.setPermission(brokerPermission);
                    // 当前分组的权限
                    if (brokerPermission.contain(Permission.READ)) {
                        groupPermission = groupPermission.addRead();
                        clusterPermission = clusterPermission.addRead();
                    }
                    if (brokerPermission.contain(Permission.WRITE)) {
                        groupPermission = groupPermission.addWrite();
                        clusterPermission = clusterPermission.addWrite();
                    }
                }

                // 设置当前分组的权限
                brokerGroup.setPermission(groupPermission);
            }
            clientIdsOfTopic.put(topic + "-PRODUCER", allProducerClientIds);
            clientIdsOfTopic.put(topic + "-CONSUMER", allConsumerClientIds);

            if (clusterPermission != Permission.NONE) {
                updateWeight(cluster, producerPolicy, dataCenter);
                result.add(cluster);
            }
        }

        // load balance&& null != cacheService
        if (config.isOpenLoadBalance() ) {
            for (BrokerCluster cluster : result) {
                String topic = cluster.getTopic();
                String topicLoadString ="";// cacheService.get(topic + "." + app);
                if (null != topicLoadString && !topicLoadString.isEmpty()) {
                    String producerSession = topic + "-PRODUCER";
                    String consumerSession = topic + "-CONSUMER";

                    // clientId not found in list, which means it hasn't been re-balanced
                    // maybe it's a new one
                    if (!clientIdsOfTopic.get(producerSession).contains(clientId)
                            && !clientIdsOfTopic.get(consumerSession).contains(clientId)) {
                        continue;
                    }

                    TopicLoad topicLoad = JSON.parseObject(topicLoadString, TopicLoad.class);
                    for (BrokerGroup bg : cluster.getGroups()) {
                        Map<String, Map<ClientType, List<String>>> topicLoads = topicLoad.getLoads();
                        if (topicLoads != null) {
                            for (String group : topicLoads.keySet()) {
                                if (!bg.getGroup().equals(group)) {
                                    continue;
                                }
                                if (clientIdsOfTopic.get(producerSession).contains(clientId) ||
                                        clientIdsOfTopic.get(consumerSession).contains(clientId)) {
                                    List<String> allowedConsumers = topicLoads.get(group).get(ClientType.CONSUMER);
                                    List<String> allowedProducers = topicLoads.get(group).get(ClientType.PRODUCER);
                                    if (null != allowedProducers && !allowedProducers.contains(clientId)) {
                                        bg.setPermission(bg.getPermission().removeWrite());
                                    }

                                    if (null != allowedConsumers && !allowedConsumers.contains(clientId)) {
                                        bg.setPermission(bg.getPermission().removeRead());
                                    }
                                }
                            }
                        }
                    }
                }

            }
        }

        return result;
    }


    // TODO: 12/7/16 by web
    // IMPORTANT： 针对 ProducerConfig/ConsumerConfig，未进行修改的变量需要设置为一个非法值
    public ConcurrentMap<String, DynamicClientConfig> getDynamicClientConfigs() {
        return null;
    }

    /**
     * 检查顺序消息是否需要重新分配可用的broker 并移除不可写的broker的写权限
     *
     * @param clusters
     */
    public void checkSequentialCluster(final List<BrokerCluster> clusters, String app) {
        sequentialManager.checkSequentialCluster(clusters, app);
    }

    /**
     * 获取顺序消息可读的broker
     *
     * @param topic 主题
     * @param app   应用
     * @return
     */
    public SequentialBrokerState.SequentialBroker getReadableBroker(String topic, String app) {
        return sequentialManager.getReadableBroker(topic, app);
    }


    /**
     * 获取权限
     *
     * @param topic      主题
     * @param app        消费者
     * @param dataCenter 客户端数据中心
     * @return 权限
     */
    public Permission getPermission(final String topic, final String app, final long dataCenter) {
        if (broker.isDisabled()) {
            return Permission.NONE;
        }
        TopicConfig topicConfig = getTopicConfig(topic);
        if (topicConfig == null) {
            return Permission.NONE;
        }
        TopicConfig.ConsumerPolicy consumerPolicy = topicConfig.getConsumerPolicy(app);
        TopicConfig.ProducerPolicy producerPolicy = topicConfig.getProducerPolicy(app);

        return getPermission(broker, dataCenter, consumerPolicy, producerPolicy);
    }


    /**
     * 获取权限
     *
     * @param broker         分组
     * @param dataCenter     客户端数据中心
     * @param consumerPolicy 消费策略
     * @param producerPolicy 生产策略
     * @return 权限
     */
    protected Permission getPermission(final Broker broker, final long dataCenter,
                                       final TopicConfig.ConsumerPolicy consumerPolicy, final TopicConfig.ProducerPolicy producerPolicy) {

        Permission permission = broker.getPermission();

        ClusterRole role = broker.getRole();
        // 如果已经在从消费列表中，则直接根据角色返回权限
        if (broker.getGroup() != null && slaveConsumeMap.containsKey(broker.getGroup())) {
            if (role.equals(ClusterRole.MASTER)) {
                return Permission.WRITE;
            } else if (role.equals(ClusterRole.SLAVE)) {
                return Permission.READ;
            }
        }
        switch (role) {
            case MASTER:
                break;
            case SLAVE:
                permission = permission.removeWrite();
                break;
            case BACKUP:
                permission = permission.removeWrite();
                break;
            default:
                permission = Permission.NONE;
        }

        // Broker被禁用了
        if (permission == Permission.NONE) {
            return permission;
        }

        // 主备模式，从节点不能消费
        if (config.isStandbyMode() && role != ClusterRole.MASTER) {
            return Permission.NONE;
        }

        // 如果没用配置消费,则不是消费者
        // 不满足消费角色，则去除读权限
        if (consumerPolicy == null || (consumerPolicy.getRole() == null && broker.getRole() != ClusterRole.MASTER) || (consumerPolicy.getRole() != null && consumerPolicy
                .getRole() != ClusterRole.NONE && broker.getRole() != consumerPolicy.getRole())) {
            permission = permission.removeRead();
        }

        // 如果没用配置生产策略，则不是生产者
        // 磁盘满了去除读权限
        if ((producerPolicy == null
                || config.getStore().isDiskFull())) {
            permission = permission.removeWrite();
        }

        return permission;
    }

    /**
     * 增加监听器
     *
     * @param listener 监听器
     */
    public void addListener(EventListener<ClusterEvent> listener) {
        if (eventManager.addListener(listener)) {
            if (isStarted()) {
                // 广播事件
                for (TopicConfig config : topics.values()) {
                    if (!config.getGroups().isEmpty() && broker.getGroup() != null && !broker.getGroup()
                            .isEmpty() && config.getGroups().contains(broker.getGroup())) {
                        //通知给存储用于比较消费者是否发生变化
                        eventManager.add(new TopicUpdateEvent(config));
                    }
                }
                eventManager.add(new RetryEvent(retryAddresses), listener);
            }
        }
    }

    /**
     * 移除监听器
     *
     * @param listener 监听器
     */
    public void removeListener(EventListener<ClusterEvent> listener) {
        eventManager.removeListener(listener);
    }

    public Permission getBrokerPermission() {
        if (broker != null) {
            return broker.getPermission();
        }
        return Permission.NONE;
    }

    public void setBrokerPermission(Permission brokerPermission) {
        if (broker != null) {
            broker.setPermission(brokerPermission);
        }
    }

    /**
     * 监听注册中心Broker变化
     */
    protected class BrokerListener implements PathListener {
        @Override
        public void onEvent(PathEvent pathEvent) {
            writeLock.lock();
            try {
                if (isStarted()) {
                    try {
                        updateBroker(pathEvent.getData());
                    } catch (IOException e) {
                        logger.error(String.format("decompress %s error.", pathEvent.getPath()), e);
                    } catch (JSONException e) {
                        logger.error(String.format("parse %s error.", pathEvent.getPath()), e);
                    }
                }
            } finally {
                writeLock.unlock();
            }

        }
    }

    /**
     * 监听数据中心变化
     */
    protected class DataCenterListener implements PathListener {
        @Override
        public void onEvent(PathEvent pathEvent) {
            writeLock.lock();
            try {
                if (isStarted()) {
                    try {
                        updateDataCenter(pathEvent.getData());
                    } catch (JSONException e) {
                        logger.error(String.format("parse %s error.", pathEvent.getPath()), e);
                    }
                }
            } finally {
                writeLock.unlock();
            }
        }
    }

    /**
     * 监听注册中心主题变化
     */
    protected class TopicListener implements PathListener {
        @Override
        public void onEvent(PathEvent pathEvent) {
            writeLock.lock();
            try {
                if (isStarted()) {
                    try {
                        updateTopic(pathEvent.getData());
                    } catch (IOException e) {
                        logger.error(String.format("decompress %s error.", pathEvent.getPath()), e);
                    } catch (JSONException e) {
                        logger.error(String.format("parse %s error.", pathEvent.getPath()), e);
                    }
                }
            } finally {
                writeLock.unlock();
            }
        }
    }

    // 负载均衡结果监听器
    /*protected class LBDataListener implements ChildrenDataListener {
        @Override
        public void onEvent(ChildrenEvent childrenEvent) {
            // 忽略删除事件
            if (childrenEvent.getType() == ChildrenEvent.ChildrenEventType.CHILD_REMOVED) {
                return;
            }

            byte[] data = childrenEvent.getData();
            if (data != null) {
                if (config.isCompressed()) {
                    try {
                        data = Compressors.decompress(data, Zip.INSTANCE);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            Map<String, List<TopicLoad>> load = JSON.parseObject(new String(data, Charsets.UTF_8), new
                    TypeReference<Map<String, List<TopicLoad>>>() {
                    });
            if (load != null) {
                for (List<TopicLoad> topicLoads : load.values()) {
                    // 更新下时间
                    for (TopicLoad tld : topicLoads) {
                        tld.setTime(SystemClock.getInstance().getTime());
                    }
                }

                // 将负载均衡结果放入内存
                loads.putAll(load);
            }
        }
    }*/
}