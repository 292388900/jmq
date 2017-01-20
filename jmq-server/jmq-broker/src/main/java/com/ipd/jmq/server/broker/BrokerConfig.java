package com.ipd.jmq.server.broker;

import com.ipd.jmq.common.cache.CacheCluster;
import com.ipd.jmq.common.network.v3.netty.NettyClient;
import com.ipd.jmq.toolkit.security.auth.Authentication;
import com.ipd.jmq.replication.ReplicationConfig;
import com.ipd.jmq.server.broker.context.BrokerContext;
import com.ipd.jmq.server.broker.offset.OffsetConfig;
import com.ipd.jmq.server.context.ContextEvent;
import com.ipd.jmq.server.broker.controller.ControllerPolicy;
import com.ipd.jmq.server.broker.controller.ControllerType;
import com.ipd.jmq.server.broker.profile.ClientStatConfig;
import com.ipd.jmq.server.store.Store;
import com.ipd.jmq.server.store.StoreConfig;
import com.ipd.jmq.server.store.StoreService;
import com.ipd.jmq.common.network.Config;
import com.ipd.jmq.common.network.ServerConfig;
import com.ipd.jmq.registry.Registry;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.retry.RetryPolicy;

import java.util.HashMap;
import java.util.Map;

/**
 * Broker服务配置
 */
public class BrokerConfig implements EventListener<ContextEvent> {
    // netty 配置选项
    protected ServerConfig serverConfig;
    // 管理服务配置项
    protected ServerConfig manageMentConfig;
    // 是否单独部署
    protected boolean standbyMode = false;
    // 数据同步配置项
    protected ReplicationConfig replicationConfig;
    // storeConfig 配置项
    protected StoreConfig storeConfig;
    // 消费位置配置
    protected OffsetConfig offsetConfig;
    // 客户端性能存储配置
    protected ClientStatConfig clientStatConfig;
    // 保存数据服务
    protected transient Store store;
    // 注册中心地址
    protected String registryUrl;
    //从web获取注册中心地址
    protected String registryInfoFromWeb;
    //webRegistry url地址，获取配置信息的WEB地址
    protected String webRegistryURL;
    // 注册中心管理
    protected transient Registry registry;
    // 当前分组
    protected String group;
    // 授权管理
    protected transient Authentication authentication;
    // 管理员用户
    protected String adminUser;
    // 管理员密码
    protected String adminPassword;

    // 缓存服务
    protected transient CacheCluster cacheService;
    // 角色决策者
    protected String roleDecider = "registry";
    // 兼容的协议
    protected String[] protocols;
    // 注册中心数据启用压缩模式
    protected boolean compressed;
    // 消费消息处理器线程数
    protected int getThreads = 32 + Runtime.getRuntime().availableProcessors() * 4;
    // 消费消息处理器线程队列容量
    protected int getQueueCapacity = 100000;
    // 生产消息处理器线程数
    protected int putThreads = 32 + Runtime.getRuntime().availableProcessors() * 4;
    // 生产消息处理器线程队列容量
    protected int putQueueCapacity = 100000;
    // 管理命令处理器线程数
    protected int adminThreads = 8;
    // 延迟调度线程数
    protected int delayThreads = 5;
    // 是否启用本地配置文件
    protected boolean useLocalConfig = false;
    // 注册中心Broker信息路径
    protected String brokerPath = "/jmq/broker_zip";
    // 注册中心主题路径
    protected String topicPath = "/jmq/topic_zip";
    // 注册中心数据中心路径
    protected String dataCenterPath = "/jmq/dataCenter";
    // 注册中心系统配置路径
    protected String configPath = "/jmq/config_zip";
    // 注册中心控制信息配置路径
    protected String controllerPath = "/jmq/controller_zip";
    //
    protected String loadbalanceDataPath = "/jmq/lb/balanced";
    // 存活节点
    protected String livePath = "/jmq/live";
    // 顺序节点
    protected String sequentialPath = "/jmq/sequential";
    // 默认应答超时时间(默认5分钟)
    protected int ackTimeout = 1000 * 60 * 5;
    //确认超时时间设置比较短就不限制客户端锁住队列的数量
    protected int limitLockMoreAckInterval = 10000;
    // 默认消费批量大小(默认10条)
    protected short batchSize = 10;
    // 最大长轮询数量
    protected int maxLongPulls = 10000;
    //messagePusher poll timeout (ms)
    protected int pusherPollTimeout = 100;
    // 检查noAckBlock超时时间间隔(默认10秒）
    protected int checkBlockExpireInterval = 1000 * 1;
    // 检查应答超时时间间隔(默认30秒）
    protected int checkAckExpireInterval = 1000 * 30;
    // 检查事务超时时间间隔(默认1秒）
    protected int checkTransactionExpireInterval = 1000;
    // 性能统计切片时间(默认1分钟)
    protected int perfStatInterval = 1000 * 60;
    // 客户端提交性能时间间隔
    protected int clientProfileInterval = 30000;
    // 客户端机器物理指标搜集时间间隔
    private int clientMachineMetricsInterval = 20000;
    // 客户端 TP 开关(关闭这个，物理指标搜集也会关闭)
    private int clientTpFlag = 1;
    // 客户端物理指标搜集开关
    private int clientMetricsFlag = 1;
    // opener for client load balance
    private boolean openLoadBalance = true;
    // 更新集群时间间隔
    protected int updateClusterInterval = 30000;

    //web注册中心拉取间隔
    protected int webRegistryInterval = 600000;
    //动态zk注册中心拉取间隔
    protected int dynamicZkRegistryInterval = 10000;
    //连续异常暂停时间ms
    protected long consumerContinueErrPauseInterval = 5000;
    //连续异常次数
    protected int consumerContinueErrs = 3;

    protected String tokenPrefix = "";
    //客户端负载均衡
    protected boolean clientLB = false;
    //客户端重平衡时间间隔
    protected long consumerReBalanceInterval = 30000;

    private Integer electionPort = 30088;
    //预取大小
    private Integer prefetchSize = 2000;
    //getClusterAck缓存时间
    private long clusterBodyCacheTime = 5000L;
    //getClusterAck大Body缓存时间
    private long clusterLargeBodyCacheTime = 30000L;
    //大Body阈值
    private long clusterLargeBodySizeThreshold = 1024 * 10;
    // netty客户端
    protected transient NettyClient nettyClient;
    // 默认重试策略
    protected RetryPolicy retryPolicy =
            new RetryPolicy(RetryPolicy.RETRY_DELAY, RetryPolicy.MAX_RETRY_DELAY, RetryPolicy.MAX_RETRYS, Boolean.TRUE,
                    RetryPolicy.BACKOFF_MULTIPLIER, RetryPolicy.EXPIRE_TIME);
    // 主题级别控制策略,key:topic
    protected Map<String, ControllerPolicy> topicControllerPolicyMap =
            new HashMap<String, ControllerPolicy>();
    // 应用级别控制策略, key:topic+@+app,添加特殊字符串防止topic与app组合有重复的
    protected Map<String, ControllerPolicy> appControllerPolicyMap =
            new HashMap<String, ControllerPolicy>();
    private String encryptKey = "example";
    private int checkMasterDeadCount = 3;

    // kafka相关配置
    // 统计的间隔时间
    protected long statTimeWindowSize = 1000*10;
    // 间隔时间内的限流次数
    protected int limitTimes = 5;
    // 默认offset metadata大小
    protected int defaultMaxMetadataSize = 4096;
    // 更新缓存时间间隔
    protected int updataKafkaCachePeriod = 10;

    public BrokerConfig store(StoreService store) {
        setStore(store);
        return this;
    }

    public String getEncryptKey() {
        return encryptKey;
    }

    public void setEncryptKey(String encryptKey) {
        if (encryptKey != null && !encryptKey.isEmpty()) {
            this.encryptKey = encryptKey;
        }
    }

    public boolean isStandbyMode() {
        return standbyMode;
    }

    public void setStandbyMode(boolean standbyMode) {
        this.standbyMode = standbyMode;
    }

    public ServerConfig getServerConfig() {
        return serverConfig;
    }

    public void setServerConfig(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    public StoreConfig getStoreConfig() {
        return storeConfig;
    }

    public void setStoreConfig(StoreConfig storeConfig) {
        this.storeConfig = storeConfig;
    }

    public Integer getElectionPort() {
        return electionPort;
    }

    public void setElectionPort(Integer electionPort) {
        this.electionPort = electionPort;
    }

    public ReplicationConfig getReplicationConfig() {
        return replicationConfig;
    }

    public void setReplicationConfig(ReplicationConfig replicationConfig) {
        this.replicationConfig = replicationConfig;
    }

    public Store getStore() {
        return store;
    }

    public void setStore(StoreService store) {
        this.store = store;
    }

    public String getRegistryUrl() {
        return registryUrl;
    }

    public void setRegistryUrl(String registryUrl) {
        this.registryUrl = registryUrl;
    }

    public String getWebRegistryURL() {
        return webRegistryURL;
    }

    public void setWebRegistryURL(String webRegistryURL) {
        this.webRegistryURL = webRegistryURL;
    }

    public Registry getRegistry() {
        return registry;
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Authentication getAuthentication() {
        return authentication;
    }

    public void setAuthentication(Authentication authentication) {
        this.authentication = authentication;
    }

    public String getAdminUser() {
        return adminUser;
    }

    public void setAdminUser(String adminUser) {
        this.adminUser = adminUser;
    }

    public String getAdminPassword() {
        return adminPassword;
    }

    public void setAdminPassword(String adminPassword) {
        this.adminPassword = adminPassword;
    }

    public CacheCluster getCacheService() {
        return cacheService;
    }

    public void setCacheService(CacheCluster cacheService) {
        this.cacheService = cacheService;
    }

    public String getRoleDecider() {
        return roleDecider;
    }

    public void setRoleDecider(String roleDecider) {
        if (roleDecider != null && !roleDecider.isEmpty()) {
            this.roleDecider = roleDecider;
        }
    }

    public int getLimitLockMoreAckInterval() {
        return limitLockMoreAckInterval;
    }

    public void setLimitLockMoreAckInterval(int limitLockMoreAckInterval) {
        this.limitLockMoreAckInterval = limitLockMoreAckInterval;
    }

    public boolean isClientLB() {
        return clientLB;
    }

    public void setClientLB(boolean clientLB) {
        this.clientLB = clientLB;
    }

    public String[] getProtocols() {
        return protocols;
    }

    public void setProtocols(String[] protocols) {
        this.protocols = protocols;
    }

    public int getGetThreads() {
        return getThreads;
    }

    public void setGetThreads(int getThreads) {
        if (getThreads > 0) {
            this.getThreads = getThreads;
        }
    }

    public int getGetQueueCapacity() {
        return getQueueCapacity;
    }

    public void setGetQueueCapacity(int getQueueCapacity) {
        if (getQueueCapacity > 0) {
            this.getQueueCapacity = getQueueCapacity;
        }
    }

    public int getPutThreads() {
        return putThreads;
    }

    public void setPutThreads(int putThreads) {
        if (putThreads > 0) {
            this.putThreads = putThreads;
        }
    }

    public int getPutQueueCapacity() {
        return putQueueCapacity;
    }

    public void setPutQueueCapacity(int putQueueCapacity) {
        if (putQueueCapacity > 0) {
            this.putQueueCapacity = putQueueCapacity;
        }
    }

    public int getAdminThreads() {
        return adminThreads;
    }

    public void setAdminThreads(int adminThreads) {
        if (adminThreads > 0) {
            this.adminThreads = adminThreads;
        }
    }

    public int getDelayThreads() {
        return delayThreads;
    }

    public void setDelayThreads(int delayThreads) {
        if (delayThreads > 0) {
            this.delayThreads = delayThreads;
        }
    }

    public String getBrokerPath() {
        return brokerPath;
    }

    public void setBrokerPath(String brokerPath) {
        if (brokerPath != null && !brokerPath.isEmpty()) {
            this.brokerPath = brokerPath;
        }
    }


    public boolean isUseLocalConfig() {
        return useLocalConfig;
    }

    public void setUseLocalConfig(boolean useLocalConfig) {
        this.useLocalConfig = useLocalConfig;
    }

    public String getTopicPath() {
        return topicPath;
    }

    public void setTopicPath(String topicPath) {
        if (topicPath != null && !topicPath.isEmpty()) {
            this.topicPath = topicPath;
        }
    }

    public void setStore(Store store) {
        this.store = store;
    }

    public String getLivePath() {
        return livePath;
    }

    public void setLivePath(String livePath) {
        this.livePath = livePath;
    }

    public String getSequentialPath() {
        return sequentialPath;
    }

    public void setSequentialPath(String sequentialPath) {
        this.sequentialPath = sequentialPath;
    }

    public String getDataCenterPath() {
        return dataCenterPath;
    }

    public void setDataCenterPath(String dataCenterPath) {
        if (dataCenterPath != null && !dataCenterPath.isEmpty()) {
            this.dataCenterPath = dataCenterPath;
        }
    }

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        if (configPath != null && !configPath.isEmpty()) {
            this.configPath = configPath;
        }
    }

    public String getControllerPath() {
        return controllerPath;
    }

    public void setControllerPath(String controllerPath) {
        this.controllerPath = controllerPath;
    }

    public String getLoadbalanceDataPath() {
        return loadbalanceDataPath;
    }

    public void setLoadbalanceDataPath(String loadbalanceDataPath) {
        this.loadbalanceDataPath = loadbalanceDataPath;
    }

    public int getAckTimeout() {
        return ackTimeout;
    }

    public void setAckTimeout(int ackTimeout) {
        if (ackTimeout > 0) {
            this.ackTimeout = ackTimeout;
        }
    }

    public short getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(short batchSize) {
        if (batchSize > 0) {
            this.batchSize = batchSize;
        }
    }

    public int getMaxLongPulls() {
        return maxLongPulls;
    }

    public void setMaxLongPulls(int maxLongPulls) {
        if (maxLongPulls > 0) {
            this.maxLongPulls = maxLongPulls;
        }
    }

    public int getCheckAckExpireInterval() {
        return checkAckExpireInterval;
    }

    public void setCheckAckExpireInterval(int checkAckExpireInterval) {
        if (checkAckExpireInterval > 0) {
            this.checkAckExpireInterval = checkAckExpireInterval;
        }
    }

    public int getCheckTransactionExpireInterval() {
        return checkTransactionExpireInterval;
    }

    public void setCheckTransactionExpireInterval(int checkTransactionExpireInterval) {
        if (checkTransactionExpireInterval > 0) {
            this.checkTransactionExpireInterval = checkTransactionExpireInterval;
        }
    }

    public int getPerfStatInterval() {
        return perfStatInterval;
    }

    public void setPerfStatInterval(int perfStatInterval) {
        if (perfStatInterval > 0) {
            this.perfStatInterval = perfStatInterval;
        }
    }

    public int getClientProfileInterval() {
        return clientProfileInterval;
    }

    public void setClientProfileInterval(int clientProfileInterval) {
        if (clientProfileInterval > 0) {
            this.clientProfileInterval = clientProfileInterval;
        }
    }

    public int getClientMachineMetricsInterval() {
        return clientMachineMetricsInterval;
    }

    public void setClientMachineMetricsInterval(int clientMachineMetricsInterval) {
        this.clientMachineMetricsInterval = clientMachineMetricsInterval;
    }

    public int getClientTpFlag() {
        return clientTpFlag;
    }

    public void setClientTpFlag(int clientTpFlag) {
        this.clientTpFlag = clientTpFlag;
    }

    public int getClientMetricsFlag() {
        return clientMetricsFlag;
    }

    public void setClientMetricsFlag(int clientMetricsFlag) {
        this.clientMetricsFlag = clientMetricsFlag;
    }

    public boolean isOpenLoadBalance() {
        return openLoadBalance;
    }

    public void setOpenLoadBalance(boolean openLoadBalance) {
        this.openLoadBalance = openLoadBalance;
    }

    public int getUpdateClusterInterval() {
        return updateClusterInterval;
    }

    public void setUpdateClusterInterval(int updateClusterInterval) {
        if (updateClusterInterval > 0) {
            this.updateClusterInterval = updateClusterInterval;
        }
    }

    public long getConsumerReBalanceInterval() {
        return consumerReBalanceInterval;
    }

    public void setConsumerReBalanceInterval(long consumerReBalanceInterval) {
        if (consumerReBalanceInterval > 0) {
            this.consumerReBalanceInterval = consumerReBalanceInterval;
        }
    }

    public void setWebRegistryInterval(int webRegistryInterval) {
        this.webRegistryInterval = webRegistryInterval;
    }

    public int getWebRegistryInterval() {
        return webRegistryInterval;
    }

    public boolean isCompressed() {
        return compressed;
    }

    public void setCompressed(boolean compressed) {
        this.compressed = compressed;
    }

    public NettyClient getNettyClient() {
        return nettyClient;
    }

    public void setNettyClient(NettyClient nettyClient) {
        this.nettyClient = nettyClient;
    }


    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public void setRetryPolicy(RetryPolicy retryPolicy) {
        if (retryPolicy != null) {
            this.retryPolicy = retryPolicy;
        }
    }

    public String getRegistryInfoFromWeb() {
        return registryInfoFromWeb;
    }

    public void setRegistryInfoFromWeb(String registryInfoFromWeb) {
        this.registryInfoFromWeb = registryInfoFromWeb;
    }

    public String getTokenPrefix() {
        return tokenPrefix;
    }

    public void setTokenPrefix(String tokenPrefix) {
        this.tokenPrefix = tokenPrefix;
    }

    public long getConsumerContinueErrPauseInterval() {
        return consumerContinueErrPauseInterval;
    }

    public void setConsumerContinueErrPauseInterval(long consumerContinueErrPauseInterval) {
        this.consumerContinueErrPauseInterval = consumerContinueErrPauseInterval;
    }

    public int getDynamicZkRegistryInterval() {
        return dynamicZkRegistryInterval;
    }

    public void setDynamicZkRegistryInterval(int dynamicZkRegistryInterval) {
        this.dynamicZkRegistryInterval = dynamicZkRegistryInterval;
    }

    public int getConsumerContinueErrs() {
        return consumerContinueErrs;
    }

    public void setConsumerContinueErrs(int consumerContinueErrs) {
        this.consumerContinueErrs = consumerContinueErrs;
    }

    public ClientStatConfig getClientStatConfig() {
        return clientStatConfig;
    }

    public void setClientStatConfig(ClientStatConfig clientStatConfig) {
        this.clientStatConfig = clientStatConfig;
    }

    public int getCheckBlockExpireInterval() {
        return checkBlockExpireInterval;
    }

    public void setCheckBlockExpireInterval(int checkBlockExpireInterval) {
        this.checkBlockExpireInterval = checkBlockExpireInterval;
    }

    public Integer getPrefetchSize() {
        return prefetchSize;
    }

    public void setPrefetchSize(Integer prefetchSize) {
        this.prefetchSize = prefetchSize;
    }

    public int getPusherPollTimeout() {
        return pusherPollTimeout;
    }

    public void setPusherPollTimeout(int pusherPollTimeout) {
        this.pusherPollTimeout = pusherPollTimeout;
    }

    public long getClusterBodyCacheTime() {
        return clusterBodyCacheTime;
    }

    public void setClusterBodyCacheTime(long clusterBodyCacheTime) {
        this.clusterBodyCacheTime = clusterBodyCacheTime;
    }

    public long getClusterLargeBodyCacheTime() {
        return clusterLargeBodyCacheTime;
    }

    public void setClusterLargeBodyCacheTime(long clusterLargeBodyCacheTime) {
        this.clusterLargeBodyCacheTime = clusterLargeBodyCacheTime;
    }

    public long getClusterLargeBodySizeThreshold() {
        return clusterLargeBodySizeThreshold;
    }

    public void setClusterLargeBodySizeThreshold(long clusterLargeBodySizeThreshold) {
        this.clusterLargeBodySizeThreshold = clusterLargeBodySizeThreshold;
    }

    public Map<String, ControllerPolicy> getTopicControllerPolicyMap() {
        return topicControllerPolicyMap;
    }

    public void setTopicControllerPolicyMap(Map<String, ControllerPolicy> topicControllerPolicyMap) {
        this.topicControllerPolicyMap = topicControllerPolicyMap;
    }

    public Map<String, ControllerPolicy> getAppControllerPolicyMap() {
        return appControllerPolicyMap;
    }

    public void setAppControllerPolicyMap(Map<String, ControllerPolicy> appControllerPolicyMap) {
        this.appControllerPolicyMap = appControllerPolicyMap;
    }

    public int getCheckMasterDeadCount() {
        return checkMasterDeadCount;
    }

    public void setCheckMasterDeadCount(int checkMasterDeadCount) {
        this.checkMasterDeadCount = checkMasterDeadCount;
    }

    public OffsetConfig getOffsetConfig() {
        return offsetConfig;
    }

    public void setOffsetConfig(OffsetConfig offsetConfig) {
        this.offsetConfig = offsetConfig;
    }

    public ServerConfig getManageMentConfig() {
        return manageMentConfig;
    }

    public void setManageMentConfig(ServerConfig manageMentConfig) {
        this.manageMentConfig = manageMentConfig;
    }

    public long getStatTimeWindowSize() {
        return statTimeWindowSize;
    }

    public void setStatTimeWindowSize(long statTimeWindowSize) {
        this.statTimeWindowSize = statTimeWindowSize;
    }

    public int getLimitTimes() {
        return limitTimes;
    }

    public void setLimitTimes(int limitTimes) {
        this.limitTimes = limitTimes;
    }

    public int getDefaultMaxMetadataSize() {
        return defaultMaxMetadataSize;
    }

    public void setDefaultMaxMetadataSize(int defaultMaxMetadataSize) {
        this.defaultMaxMetadataSize = defaultMaxMetadataSize;
    }

    public int getUpdataKafkaCachePeriod() {
        return updataKafkaCachePeriod;
    }

    public void setUpdataKafkaCachePeriod(int updataKafkaCachePeriod) {
        this.updataKafkaCachePeriod = updataKafkaCachePeriod;
    }

    @Override
    public void onEvent(ContextEvent event) {
        if (event == null) {
            return;
        }

        String key = event.getKey();
        String value = event.getValue();
        String group = null;
        int pos = key.indexOf('.');
        if (pos > 0) {
            group = key.substring(0, pos);
        }
        if (group != null && (group.equals(ContextEvent.ARCHIVE_GROUP) || group.equals(ContextEvent.STORE_GROUP))) {
            if (storeConfig != null) {
                storeConfig.onEvent(event);
            }
        } else if (group != null && group.equals(ContextEvent.HA_GROUP)) {
            // TODO replication config event
        } else if (group != null && group.equals(ContextEvent.RETRY_SERVER_GROUP)) {
//            if (retryConfig != null) {
//                retryConfig.onEvent(event);
//            }
        } else if (group != null && group.equals(BrokerContext.NETTY_SERVER_GROUP)) {
            if (serverConfig != null) {
                ServerConfig nsConfig = this.serverConfig;
                if (BrokerContext.SERVER_SEND_TIMEOUT.equals(key)) {
                    nsConfig.setSendTimeout(event.getPositive(nsConfig.getSendTimeout()));
                } else if (BrokerContext.SERVER_WORKER_THREADS.equals(key)) {
                    nsConfig.setWorkerThreads(event.getPositive(nsConfig.getWorkerThreads()));
                } else if (BrokerContext.SERVER_CALLBACK_EXECUTOR_THREADS.equals(key)) {
                    nsConfig.setCallbackThreads(event.getPositive(nsConfig.getCallbackThreads()));
                } else if (BrokerContext.SERVER_SELECTOR_THREADS.equals(key)) {
                    nsConfig.setSelectorThreads(event.getPositive(nsConfig.getSelectorThreads()));
                } else if (BrokerContext.SERVER_CHANNEL_MAX_IDLE_TIME.equals(key)) {
                    nsConfig.setMaxIdleTime(event.getPositive(nsConfig.getMaxIdleTime()));
                } else if (BrokerContext.SERVER_SO_TIMEOUT.equals(key)) {
                    nsConfig.setSoTimeout(event.getPositive(nsConfig.getSoTimeout()));
                } else if (BrokerContext.SERVER_SOCKET_BUFFER_SIZE.equals(key)) {
                    nsConfig.setSocketBufferSize(event.getPositive(nsConfig.getSocketBufferSize()));
                } else if (BrokerContext.SERVER_MAX_ONEWAY.equals(key)) {
                    nsConfig.setMaxOneway(event.getPositive(nsConfig.getMaxOneway()));
                } else if (BrokerContext.SERVER_MAX_ASYNC.equals(key)) {
                    nsConfig.setMaxAsync(event.getPositive(nsConfig.getMaxAsync()));
                } else if (BrokerContext.SERVER_BACKLOG.equals(key)) {
                    nsConfig.setBacklog(event.getPositive(nsConfig.getBacklog()));
                }
            }
        } else if (group != null && group.equals(BrokerContext.NETTY_CLIENT_GROUP)) {
            if (nettyClient != null) {
                Config ncConfig = nettyClient.getConfig();
                if (BrokerContext.CLIENT_SEND_TIMEOUT.equals(key)) {
                    ncConfig.setSendTimeout(event.getPositive(ncConfig.getSendTimeout()));
                } else if (BrokerContext.CLIENT_WORKER_THREADS.equals(key)) {
                    ncConfig.setWorkerThreads(event.getPositive(ncConfig.getWorkerThreads()));
                } else if (BrokerContext.CLIENT_CALLBACK_EXECUTOR_THREADS.equals(key)) {
                    ncConfig.setCallbackThreads(event.getPositive(ncConfig.getCallbackThreads()));
                } else if (BrokerContext.CLIENT_SELECTOR_THREADS.equals(key)) {
                    ncConfig.setSelectorThreads(event.getPositive(ncConfig.getSelectorThreads()));
                } else if (BrokerContext.CLIENT_CHANNEL_MAX_IDLE_TIME.equals(key)) {
                    ncConfig.setMaxIdleTime(event.getPositive(ncConfig.getMaxIdleTime()));
                } else if (BrokerContext.CLIENT_SO_TIMEOUT.equals(key)) {
                    ncConfig.setSoTimeout(event.getPositive(ncConfig.getSoTimeout()));
                } else if (BrokerContext.CLIENT_SOCKET_BUFFER_SIZE.equals(key)) {
                    ncConfig.setSocketBufferSize(event.getPositive(ncConfig.getSocketBufferSize()));
                } else if (BrokerContext.CLIENT_MAX_ONEWAY.equals(key)) {
                    ncConfig.setMaxOneway(event.getPositive(ncConfig.getMaxOneway()));
                } else if (BrokerContext.CLIENT_MAX_ASYNC.equals(key)) {
                    ncConfig.setMaxAsync(event.getPositive(ncConfig.getMaxAsync()));
                } else if (BrokerContext.CLIENT_CONNECTION_TIMEOUT.equals(key)) {
                    ncConfig.setSendTimeout(event.getPositive(ncConfig.getSendTimeout()));
                }
            }
        } else if (group != null && group.equals(BrokerContext.RETRY_GROUP)) {
            if (BrokerContext.RETRY_MAX_RETRYS.equals(key)) {
                retryPolicy.setMaxRetrys(event.getInt(retryPolicy.getMaxRetrys()));
            } else if (BrokerContext.RETRY_MAX_RETRY_DELAY.equals(key)) {
                retryPolicy.setMaxRetryDelay(event.getInt(retryPolicy.getMaxRetryDelay()));
            } else if (BrokerContext.RETRY_RETRY_DELAY.equals(key)) {
                retryPolicy.setRetryDelay(event.getInt(retryPolicy.getRetryDelay()));
            } else if (BrokerContext.RETRY_USE_EXPONENTIAL_BACK_OFF.equals(key)) {
                retryPolicy.setUseExponentialBackOff(event.getBoolean(retryPolicy.getUseExponentialBackOff()));
            } else if (BrokerContext.RETRY_BACK_OFF_MULTIPLIER.equals(key)) {
                retryPolicy.setBackOffMultiplier(event.getDouble(retryPolicy.getBackOffMultiplier()));
            } else if (BrokerContext.RETRY_EXPIRE_TIME.equals(key)) {
                retryPolicy.setExpireTime(event.getInt(retryPolicy.getExpireTime()));
            }
        } else if (group != null && group.equals(BrokerContext.CONTROL_GROUP)) {
            if (BrokerContext.BROKER_TOPIC_CONTROLLER_PERMISSION.equals(key)) {
                ControllerPolicy controllerPolicy = new ControllerPolicy(ControllerType.TOPIC_BROKER_PERMISSION, value, this.group);
                topicControllerPolicyMap.put(controllerPolicy.getTopic(), controllerPolicy);
            } else if (BrokerContext.BROKER_APP_CONTROLLER_CLIENTIP.equals(key)) {
                ControllerPolicy controllerPolicy = new ControllerPolicy(ControllerType.APP_CLIENT_IP_LIMIT, value, this.group);
                appControllerPolicyMap.put(controllerPolicy.getTopic() + "@" + controllerPolicy.getApp(), controllerPolicy);
            } else if (BrokerContext.DELETE_OLD_TOPIC_CONTROLLER_DATA.equals(key)) {
                topicControllerPolicyMap.remove(value);
            } else if (BrokerContext.DELETE_OLD_APP_CONTROLLER_DATA.equals(key)) {
                appControllerPolicyMap.remove(value);
            } else if (BrokerContext.CLEAR_ALL_CONTROLLER_DATA.equals(key)) {
                topicControllerPolicyMap.clear();
                appControllerPolicyMap.clear();
            }
        } else if (BrokerContext.BROKER_CLIENT_PROFILE_INTERVAL.equals(key)) {
            setClientProfileInterval(event.getPositive(clientProfileInterval));
        } else if (BrokerContext.BROKER_CLIENT_MACHINE_METRICS_INTERVAL.equals(key)) {
            setClientMachineMetricsInterval(event.getPositive(clientMachineMetricsInterval));
        } else if (BrokerContext.BROKER_CLIENT_TP_FLAG.equals(key)) {
            setClientTpFlag(event.getInt(clientTpFlag));
        } else if (BrokerContext.BROKER_CLIENT_METRICS_FLAG.equals(key)) {
            setClientMetricsFlag(event.getInt(clientMetricsFlag));
        } else if (BrokerContext.BROKER_CLIENT_LOAD_BALANCE_OPENER.equals(key)) {
            setOpenLoadBalance(event.getBoolean(openLoadBalance));
        } else if (BrokerContext.BROKER_UPDATE_CLUSTER_INTERVAL.equals(key)) {
            setUpdateClusterInterval(event.getPositive(updateClusterInterval));
        } else if (BrokerContext.BROKER_ADMIN_USER.equals(key)) {
            setAdminUser(event.getValue());
        } else if (BrokerContext.BROKER_ADMIN_PASSWORD.equals(key)) {
            setAdminPassword(event.getValue());
        } else if (BrokerContext.BROKER_ROLE_DECIDER.equals(key)) {
            setRoleDecider(event.getValue());
        } else if (BrokerContext.BROKER_GET_THREADS.equals(key)) {
            setGetThreads(event.getPositive(getThreads));
        } else if (BrokerContext.BROKER_GET_QUEUE_CAPACITY.equals(key)) {
            setGetQueueCapacity(event.getPositive(getQueueCapacity));
        } else if (BrokerContext.BROKER_PUT_THREADS.equals(key)) {
            setPutThreads(event.getPositive(putThreads));
        } else if (BrokerContext.BROKER_PUT_QUEUE_CAPACITY.equals(key)) {
            setPutQueueCapacity(event.getPositive(putQueueCapacity));
        } else if (BrokerContext.BROKER_ADMIN_THREADS.equals(key)) {
            setAdminThreads(event.getPositive(adminThreads));
        } else if (BrokerContext.BROKER_ACK_TIMEOUT.equals(key)) {
            setAckTimeout(event.getPositive(ackTimeout));
        } else if (BrokerContext.BROKER_BATCH_SIZE.equals(key)) {
            setBatchSize((short) event.getPositive((int) batchSize));
        } else if (BrokerContext.BROKER_MAX_LONG_PULLS.equals(key)) {
            setMaxLongPulls(event.getPositive(maxLongPulls));
        } else if (BrokerContext.BROKER_CHECK_ACK_EXPIRE_INTERVAL.equals(key)) {
            setCheckAckExpireInterval(event.getPositive(checkAckExpireInterval));
        } else if (BrokerContext.BROKER_CHECK_TRANSACTION_EXPIRE_INTERVAL.equals(key)) {
            setCheckTransactionExpireInterval(event.getPositive(checkTransactionExpireInterval));
        } else if (BrokerContext.BROKER_PERF_STAT_INTERVAL.equals(key)) {
            setPerfStatInterval(event.getPositive(perfStatInterval));
        } else if (BrokerContext.BROKER_CONSUMER_CONTINUE_ERR_PAUSE_INTERVAL.equals(key)) {
            setConsumerContinueErrPauseInterval(event.getPositive(consumerContinueErrPauseInterval));
        } else if (BrokerContext.BROKER_CONSUMER_CONTINUE_ERR_PAUSE_TIMES.equals(key)) {
            setConsumerContinueErrs(event.getPositive(consumerContinueErrs));
        } else if (BrokerContext.BROKER_CONSUMER_REBALANCE_INTERVAL.equals(key)) {
            setConsumerReBalanceInterval(event.getPositive(consumerReBalanceInterval));
        } else if (BrokerContext.BROKER_CLUSTER_BODY_CACHE_TIME.equals(key)) {
            setClusterBodyCacheTime(event.getPositive(clusterBodyCacheTime));
        } else if (BrokerContext.BROKER_CLUSTER_LARGEBODY_CACHE_TIME.equals(key)) {
            setClusterLargeBodyCacheTime(event.getPositive(clusterLargeBodyCacheTime));
        } else if (BrokerContext.BROKER_CLUSTER_LARGEBODY_SIZE_THRESHOLD.equals(key)) {
            setClusterLargeBodySizeThreshold(event.getPositive(clusterLargeBodySizeThreshold));
        }
    }
}
