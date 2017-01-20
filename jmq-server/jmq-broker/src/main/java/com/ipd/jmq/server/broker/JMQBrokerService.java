package com.ipd.jmq.server.broker;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.cluster.ClusterRole;
import com.ipd.jmq.common.network.ServerConfig;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportEvent;
import com.ipd.jmq.common.network.v3.netty.NettyClient;
import com.ipd.jmq.common.network.v3.session.Connection;
import com.ipd.jmq.replication.*;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.broker.cluster.SequentialManager;
import com.ipd.jmq.server.broker.handler.telnet.PermiQueryHandler;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
//import com.ipd.jmq.election.RaftRoleDecider;
import  com.ipd.jmq.server.broker.election.RoleDecider;
import  com.ipd.jmq.server.broker.election.RoleEvent;
import com.ipd.jmq.server.broker.handler.*;
import com.ipd.jmq.common.network.v3.netty.telnet.base.AuthHandler;
import com.ipd.jmq.server.broker.handler.telnet.MonitorHandler;
import com.ipd.jmq.server.broker.monitor.BrokerMonitor;
import com.ipd.jmq.server.broker.netty.ManagementServer;
import com.ipd.jmq.server.broker.netty.MessagingServer;
import com.ipd.jmq.server.broker.netty.protocol.MessagingProtocol;
import com.ipd.jmq.server.broker.offset.OffsetManager;
//import com.ipd.jmq.server.broker.retry.RetryManagerDummy;
import com.ipd.jmq.server.broker.service.BrokerService;
import com.ipd.jmq.server.broker.utils.BrokerUtils;
import com.ipd.jmq.server.context.ContextConfig;
import com.ipd.jmq.server.context.ContextManager;
import com.ipd.jmq.server.broker.dispatch.DispatchManager;
import com.ipd.jmq.server.broker.profile.ClientStatManager;
import com.ipd.jmq.server.broker.registry.DynamicZkRegistry;
import com.ipd.jmq.server.broker.registry.WebRegistry;
//import com.ipd.jmq.server.broker.retry.RetryManager;
import com.ipd.jmq.server.store.*;
import com.ipd.jmq.common.network.netty.Protocol;
import com.ipd.jmq.common.network.protocol.telnet.ClearHandler;
import com.ipd.jmq.common.network.protocol.telnet.ExitHandler;
import com.ipd.jmq.common.network.protocol.telnet.HelpHandler;
import com.ipd.jmq.registry.Registry;
import com.ipd.jmq.registry.RegistryFactory;
import com.ipd.jmq.registry.listener.ConnectionEvent;
import com.ipd.jmq.registry.listener.ConnectionListener;
import com.ipd.jmq.registry.zookeeper.ZKRegistry;
import com.ipd.jmq.toolkit.URL;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.concurrent.NamedThreadFactory;
import com.ipd.jmq.toolkit.concurrent.Scheduler;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.network.Ipv4;
import com.ipd.jmq.toolkit.plugin.PluginUtil;
import com.ipd.jmq.toolkit.security.auth.DefaultAuthentication;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.service.ServiceThread;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Broker服务
 */
public class JMQBrokerService extends Service {
    protected static Logger logger = LoggerFactory.getLogger(JMQBrokerService.class);
    // 配置
    protected BrokerConfig config;
    // 集群角色决策者
    protected RoleDecider roleDecider;
    // 集群信息管理
    protected ClusterManager clusterManager;
    // 上下文环境配置
    protected ContextManager contextManager;
    // 会话管理
    protected SessionManager sessionManager;
    // 事务管理
    protected TxTransactionManager transactionManager;
    // 消费消息线程池
    protected ExecutorService getExecutor;
    // 发送消息的线程池
    protected ExecutorService putExecutor;
    // 长轮询管理
    protected LongPullManager longPullManager;
    // Netty服务
    protected MessagingServer nettyServer;
    //管理服务
    protected ManagementServer managementServer;
    // 重试管理
//    protected RetryManager retryManager;
    // 当前Broker
    protected Broker broker;
    // 数据分发服务
    protected DispatchService dispatchService;
    // 延迟调度服务
    protected Scheduler scheduler;
    // 存储服务
    protected Store store;
    // 清理服务
    protected CleanupManager cleanupManager;
    // 复制主服务
    protected ReplicationMasterService replicationMaster;
    // 复制从服务
    protected ReplicationSlaveService replicationSlave;
    // WEB配置信息任务调度
    protected ScheduledExecutorService fixedRateSchedule = Executors.newScheduledThreadPool(1,
            new NamedThreadFactory("JMQ_SERVER_WEB_REGISTRY_SCHEDULE"));
    // 动态zk地址调度器
    protected ScheduledExecutorService dynaZkRegistrySchedule = Executors.newScheduledThreadPool(1,
            new NamedThreadFactory("JMQ_SERVER_DYNAMIC_ZK_REGISTRY_SCHEDULE"));

    // 角色选举监听器
    protected RoleEventListener roleEventListener = new RoleEventListener();

    protected ReplicationListener haListener = new HAListener();

    // Netty事件监听器
    protected EventListener<TransportEvent> transportEventListener = new TransportEventListener();
    // broker监控
    protected BrokerMonitor brokerMonitor;
    // 归档日志
//    protected ArchiveManager archiveManager;
    // 客户端性能统计
    protected ClientStatManager clientStatManager;
    // 顺序消息管理器
    protected SequentialManager sequentialManager;
    // 命令处理器工厂
    protected DefaultHandlerFactory handlerFactory;
    // 迁移管理器
    protected LocalOffsetManager localOffsetManager;

    protected ServiceThread flushWaterMarkThread;
    protected AtomicBoolean flushWaterMarkFlag = new AtomicBoolean(false);
    protected volatile long waterMark;
    protected long waterMarkTimestamp;
    protected StoreUnsafe storeUnsafe;
    protected File waterMarkPersistentFile;
    protected OffsetManager offsetManager;

    // 成功复制的JournalOffset
    protected volatile long replicatedJournalOffset;
    // 成功刷的JournalOffset
    protected volatile long flushedJournalOffset;

    // 兼容协议
    protected List<BrokerService> brokerServices;
    protected List<Protocol> protocols = new CopyOnWriteArrayList<Protocol>();

    public JMQBrokerService() {
    }

    public JMQBrokerService(BrokerConfig config) {
        this.config = config;
    }

    public void setConfig(BrokerConfig config) {
        this.config = config;
    }

    private List<Protocol> loadProtocols() {
        List<MessagingProtocol> loadPlugins = PluginUtil.loadPlugins(MessagingProtocol.class);
        if (loadPlugins == null || loadPlugins.isEmpty()) {
            logger.error("No available protocols!!");
            return null;
        }

        for (MessagingProtocol protocol : loadPlugins) {
            protocol.setFactory(handlerFactory);
            this.protocols.add(protocol);
        }
        if (this.protocols.isEmpty()) {
            logger.error("No available protocols!!");
        }

        return this.protocols;
    }

    /**
     * 验证
     *
     * @throws java.lang.Exception
     */
    protected void validate() throws Exception {
        super.validate();
        Preconditions.checkArgument(config != null, "config can not be null");

        ServerConfig nettyServerConfig = config.getServerConfig();
        Preconditions.checkArgument(nettyServerConfig != null, "config is invalid. nettServerConfig can not be null");

        // 设置本地地址
        if (nettyServerConfig.getIp() == null || nettyServerConfig.getIp().isEmpty()) {
            nettyServerConfig.setIp(Ipv4.getLocalIp());
        }


        if (waterMarkPersistentFile == null) {
            waterMarkPersistentFile = new File(config.getConfigPath() + "/water_mark.save");
        }

        NettyClient nettyClient = config.getNettyClient();

        Preconditions.checkState(nettyClient != null, "config is invalid. nettyClient can not be null");

      /*  // 纠正客户端数据包大小，便于同步数据块
        ClientConfig nettyConfig = nettyClient.getConfig();
        if (haConfig.getBlockSize() + 1024 > nettyConfig.getFrameMaxSize()) {
            nettyConfig.setFrameMaxSize(haConfig.getBlockSize() + 1024);
        }*/
        // 打开注册中心
        if (config.getRegistry() == null) {
            Preconditions.checkState(config.getRegistryUrl() != null && !config.getRegistryUrl().isEmpty(),
                    "config is invalid. registry & registryUrl can not be both null");
            Registry registry = null;


            RegistryFactory factory = new RegistryFactory(config.getRegistryUrl());
            if (factory != null) {
                registry = factory.create();
            }
            if (registry == null) {
                registry = PluginUtil.createService(Registry.class, URL.valueOf(config.getRegistryUrl()));
            }
            Preconditions.checkState(registry != null, "config is invalid. registryUrl is not implement");

            if (config.getRegistryInfoFromWeb() != null && !config.getRegistryInfoFromWeb().isEmpty() && registry instanceof ZKRegistry) {
                DynamicZkRegistry dynamicZkRegistry = new DynamicZkRegistry((ZKRegistry) registry);
                dynamicZkRegistry.setRegistryInfoURL(config.getRegistryInfoFromWeb());
                //拉取web注册中心数据
                dynaZkRegistrySchedule.scheduleAtFixedRate(dynamicZkRegistry, config.getDynamicZkRegistryInterval(), config.getDynamicZkRegistryInterval(), TimeUnit.MILLISECONDS);
                registry = dynamicZkRegistry;
            }

            config.setRegistry(registry);
        }
        if (contextManager == null) {
            contextManager = new ContextManager(new ContextConfig(config.getConfigPath(), config.isCompressed(), config.getRegistry()));
        }

        if (sequentialManager == null) {
            sequentialManager = new SequentialManager(config);
        }
        // 创建集群管理
        if (clusterManager == null) {
            clusterManager = new ClusterManager(config);
            clusterManager.setSequentialManager(sequentialManager);
            sequentialManager.setClusterManager(clusterManager);
        }
        if (broker == null) {
            // 得到本地Broker对象
            broker = clusterManager.getBroker();
        }
        if (broker.getGroup() != null && !broker.getGroup().isEmpty()) {
            config.setGroup(broker.getGroup());
        }

        if (roleDecider == null) {
            String decider = config.getRoleDecider();
            if (!decider.contains("://")) {
                decider = decider + "://";
            }
            roleDecider = PluginUtil.createService(RoleDecider.class, URL.valueOf(decider));
            Preconditions.checkState(roleDecider != null, "config is invalid. roleDecider is not implement.");
            roleDecider.setClusterManager(clusterManager);
            roleDecider.setBrokerConfig(config);
        }

        // 权限认证
        if (config.getAuthentication() == null) {
            config.setAuthentication(new DefaultAuthentication(config.getAdminUser(), config.getAdminPassword(), config.getTokenPrefix()));
        } else if (config.getAuthentication() instanceof DefaultAuthentication) {
            DefaultAuthentication defAuth = (DefaultAuthentication) config.getAuthentication();
            if (config.getAdminUser() == null || config.getAdminUser().isEmpty()) {
                config.setAdminUser(defAuth.getAdminUser());
            }
            if (config.getAdminPassword() == null || config.getAdminPassword().isEmpty()) {
                config.setAdminPassword(defAuth.getAdminPassword());
            }
        }

        if (sessionManager == null) {
            sessionManager = new SessionManager();
            sessionManager.setSequentialManager(sequentialManager);
            sequentialManager.setSessionManager(sessionManager);
        }

        if (scheduler == null) {
            scheduler = new Scheduler(config.getDelayThreads());
            scheduler.start();
        }

        StoreConfig storeConfig = config.getStoreConfig();
        if (config.getStore() == null && storeConfig == null) {
            throw new IllegalStateException("config is invalid. storeConfig or store can not be both null");
        } else if (config.getStore() == null) {
            JMQStore jmqStore = new JMQStore(storeConfig, clusterManager.getBroker());
            StoreService storeService = new StoreService(storeConfig, clusterManager.getBroker(), clusterManager);
            storeService.setStore(jmqStore);
            config.setStore(storeService);
            this.store = storeService;
            this.storeUnsafe = storeService;
            jmqStore.addListener(new StoreListener() {
                @Override
                public void onEvent(StoreEvent storeEvent) {
                    if (storeEvent.getType() == StoreEvent.EventType.FLUSHED) {
                        flushedJournalOffset = storeEvent.getFlushedJournalOffset();
                        updateWaterMark(flushedJournalOffset);
//                        updateWaterMark(Math.min(replicatedJournalOffset, flushedJournalOffset));
                    }
                }
            });
        } else {
            this.store = config.getStore();
            this.storeUnsafe = (StoreUnsafe) store;
            if (config.getStoreConfig() == null) {
                config.setStoreConfig(config.getStore().getConfig());
                storeConfig = config.getStoreConfig();
            }
        }

        Preconditions.checkState(storeUnsafe != null, "storeUnsafe is null, that will be mis function on replication.");

        if (storeConfig.getMaxDiskSpace() <= 0) {
            // 计算存储占用硬盘最大空间
            File file = storeConfig.getDataDirectory();
            long space = file.getTotalSpace() * storeConfig.getMaxDiskSpaceUsage() / 100;
            storeConfig.setMaxDiskSpace(space);
        }

        if (offsetManager == null) {
            offsetManager = new OffsetManager(config.getOffsetConfig());
            offsetManager.setStore(store);
            offsetManager.setClusterManager(clusterManager);
        }

        if (replicationMaster == null) {
            ReplicationConfig repConfig = config.getReplicationConfig();
            ServerConfig serverConfig = repConfig.getServerConfig();
            if (serverConfig == null) {
                serverConfig = new ServerConfig();
                serverConfig.setSocketBufferSize(1024000);
                repConfig.setServerConfig(serverConfig);
            }
            serverConfig.setIp(broker.getIp());
            serverConfig.setPort(broker.getReplicationPort());
            serverConfig.setEpoll(this.config.getServerConfig().isEpoll());

            replicationMaster = new ReplicationMasterService(broker, repConfig, haListener, store);
        }

        // 创建客户端性能统计
        if (clientStatManager == null) {
            String type = config.getClientStatConfig().getClientStatType();
            if (type != null && !type.isEmpty()) {
                if (type.indexOf("://") == -1) {
                    type = type + "://";
                }
                clientStatManager = PluginUtil.createService(ClientStatManager.class, URL.valueOf(type));
                clientStatManager.setConfig(config.getClientStatConfig());
                clientStatManager.setClusterManager(clusterManager);
            }
        }
        if (clientStatManager == null) {
            logger.warn("clientStatManager is not configured.");
        }

        if (getExecutor == null) {
            getExecutor = new ThreadPoolExecutor(config.getGetThreads(), config.getGetThreads(), 1000 * 60,
                    TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(config.getGetQueueCapacity()),
                    new NamedThreadFactory("JMQ_SERVER_PULL_EXECUTOR"));
        }
        if (putExecutor == null) {
            putExecutor = new ThreadPoolExecutor(config.getPutThreads(), config.getPutThreads(), 1000 * 60,
                    TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(config.getPutQueueCapacity()),
                    new NamedThreadFactory("JMQ_SERVER_PUT_EXECUTOR"));
        }
//        if (retryManager == null) {
//            retryManager = new RetryManagerDummy(clusterManager, contextManager, scheduler, config);
//        }

        if (dispatchService == null) {
            dispatchService = new DispatchManager(sessionManager, clusterManager, config, offsetManager);
            sequentialManager.setDispatchService(dispatchService);
            ((StoreService) store).setDispatchService(dispatchService);
        }

        if (brokerMonitor == null) {
            brokerMonitor = new BrokerMonitor(sessionManager, clusterManager, dispatchService, config,
                    replicationMaster, SystemClock.now());
            dispatchService.setBrokerMonitor(brokerMonitor);
            clusterManager.setBrokerMonitor(brokerMonitor);
            sequentialManager.setBrokerMonitor(brokerMonitor);
        }

//        if (archiveManager == null) {
//            archiveManager = new ArchiveManager(storeConfig);
//        }
        if (longPullManager == null) {
            longPullManager = new LongPullManager(getExecutor, sessionManager, clusterManager, dispatchService, config, brokerMonitor);
        }
        if (transactionManager == null) {
            transactionManager = new TxTransactionManager(clusterManager, config, brokerMonitor, getExecutor, sessionManager);
        }

        if (handlerFactory == null) {
            GetMessageHandler getMessageHandler =
                    new GetMessageHandler(getExecutor, sessionManager, clusterManager, dispatchService, longPullManager,
                            brokerMonitor, config);
            AckMessageHandler ackMessageHandler =
                    new AckMessageHandler(getExecutor, sessionManager, clusterManager, dispatchService,config.getStore(), brokerMonitor);
//            RetryMessageHandler retryMessageHandler =
//                    new RetryMessageHandler(getExecutor, sessionManager, clusterManager, dispatchService, config, scheduler, brokerMonitor);
            SessionHandler sessionHandler =
                    new SessionHandler(putExecutor, sessionManager, clusterManager, config, clientStatManager, brokerMonitor);
            PutMessageHandler putMessageHandler = new PutMessageHandler(putExecutor, sessionManager, clusterManager, dispatchService,
                    transactionManager, config, brokerMonitor);
            TxTransactionHandler txTransactionHandler = new TxTransactionHandler(putExecutor, sessionManager,
                    clusterManager, transactionManager);
            TxFeedbackHandler feedbackHandler = new TxFeedbackHandler(putExecutor, sessionManager, clusterManager,
                    transactionManager);
            MetadataHandler metadataHandler = new MetadataHandler(putExecutor, sessionManager, clusterManager,
                    config, brokerMonitor);

            // 注册JMQ handlers
            handlerFactory = new DefaultHandlerFactory().register(ackMessageHandler).register(getMessageHandler)
                    .register(sessionHandler).register(txTransactionHandler)
                    .register(putMessageHandler).register(feedbackHandler).register(metadataHandler);
        }

        if (nettyServer == null) {
            protocols = loadProtocols();
            nettyServer = new MessagingServer(config.getServerConfig(), null, null, null, protocols);
            if (config.getProtocols() != null) {
                if (brokerServices == null) {
                    brokerServices = new ArrayList<BrokerService>();
                }
                for (String value : config.getProtocols()) {
                    if (!value.contains("://")) {
                        value = value + "://";
                    }
                    BrokerService brokerService = PluginUtil.createService(BrokerService.class, URL.valueOf(value));
                    if (brokerService == null) {
                        continue;
                    }

                    brokerService.setBrokerConfig(config);
                    brokerService.setStore(store);
                    brokerService.setClusterManager(clusterManager);
                    brokerService.setSessionManager(sessionManager);
                    brokerService.setScheduler(scheduler);
                    brokerService.setRoleDecider(roleDecider);
                    brokerService.setDispatchService(dispatchService);
                    brokerService.setCommandHandlerFactory(handlerFactory);
                    brokerServices.add(brokerService);
                }
            }
            nettyServer.addListener(transportEventListener);
        }

        if (managementServer == null) {
            ServerConfig serverConfig = config.getManageMentConfig();
            if (serverConfig == null) {
                serverConfig = new ServerConfig();
                config.setManageMentConfig(serverConfig);
            }
            serverConfig.setIp(broker.getIp());
            serverConfig.setPort(broker.getManagementPort());
            serverConfig.setEpoll(this.config.getServerConfig().isEpoll());
            serverConfig.setSelectorThreads(2);
            serverConfig.setWorkerThreads(5);

            // telnet handler
            AuthHandler authHandler = new AuthHandler(config.getAdminUser(), config.getAdminPassword());
            MonitorHandler monitorHandler = new MonitorHandler(brokerMonitor, config.getAdminUser(), config.getAdminPassword());
            PermiQueryHandler permiQueryHandler = new PermiQueryHandler(clusterManager);

            // 注册Telnet handlers
            TelnetHandlerFactory telnetFactory = new TelnetHandlerFactory().register(new HelpHandler()).register(new ExitHandler())
                    .register(new ClearHandler()).register(authHandler).register(monitorHandler).register(permiQueryHandler);
            managementServer = new ManagementServer(serverConfig, null, null, null, telnetFactory);
        }

        if (localOffsetManager == null) {
            localOffsetManager = new LocalOffsetManager(config, clusterManager, dispatchService);
        }

        fixedRateSchedule.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if (getExecutor != null && getExecutor instanceof ThreadPoolExecutor) {
                    int queueSize = ((ThreadPoolExecutor) getExecutor).getQueue().size();
                    int activeThreads = ((ThreadPoolExecutor) getExecutor).getActiveCount();
                    logger.info(String.format("getExecutor wait queue:%d,activeThreads:%d", queueSize, activeThreads));
                }
                if (putExecutor != null && putExecutor instanceof ThreadPoolExecutor) {
                    int queueSize = ((ThreadPoolExecutor) putExecutor).getQueue().size();
                    int activeThreads = ((ThreadPoolExecutor) putExecutor).getActiveCount();
                    logger.info(String.format("putExecutor wait queue:%d,activeThreads:%d", queueSize, activeThreads));
                }

            }
        }, 10000, 60000, TimeUnit.MILLISECONDS);

        if (flushWaterMarkThread == null) {
            flushWaterMarkThread = new ServiceThread(this, 1) {
                @Override
                protected void execute() throws Exception {
                    if (flushWaterMarkFlag.get()) {
                        BrokerUtils.writeConfigFile(waterMarkPersistentFile, waterMark);
                        flushWaterMarkFlag.set(false);
                    }
                }

                @Override
                public boolean onException(Throwable e) {
                    logger.error("save waterMark to {} failed: ", waterMarkPersistentFile.toString(), e);
                    return true;
                }
            };
        }
    }

    @Override
    protected void doStart() throws Exception {
        // 加锁，防止关闭和集群选举事件
        super.doStart();
        logger.info("wait until service is connected to registry.");

        waterMark = (Long) BrokerUtils.readConfigFile(waterMarkPersistentFile, Long.class, 0L);
        flushedJournalOffset = replicatedJournalOffset = waterMark;

        RegistryListener registryListener;
        // 确保注册中心连接上
        final CountDownLatch latch = new CountDownLatch(1);
        registryListener = new RegistryListener(latch);
        config.getRegistry().addListener(registryListener);
        config.getRegistry().start();
        if (config.isUseLocalConfig()) {
            //使用本地不需要等待连接
            latch.await(20000L, TimeUnit.MILLISECONDS);
        } else {
            latch.await();
        }
        config.getRegistry().removeListener(registryListener);
        logger.info("success connecting to registry:" + config.getRegistry().getUrl());

        contextManager.addListener(config);
        contextManager.start();

        if (config.getRegistry() != null) {
            if (config.getRegistry() instanceof WebRegistry) {
                //拉取web注册中心数据
                fixedRateSchedule.scheduleAtFixedRate((WebRegistry) config.getRegistry(), config.getWebRegistryInterval(), config.getWebRegistryInterval(), TimeUnit.MILLISECONDS);
            } else {
                if (config.getWebRegistryURL() != null && config.getWebRegistryURL().length() > 0) {
                    WebRegistry fixFetchRegistry = new WebRegistry(config.getWebRegistryURL());
                    fixFetchRegistry.setCompress(config.isCompressed());
                    clusterManager.addMetaConfigListener(fixFetchRegistry);
                    fixedRateSchedule.scheduleAtFixedRate(fixFetchRegistry, config.getWebRegistryInterval(), config.getWebRegistryInterval(), TimeUnit.MILLISECONDS);
                }
            }
        }

        managementServer.start();

        store.start();

        offsetManager.start();

        cleanupManager = new CleanupManager(store, offsetManager);

        cleanupManager.start();

        config.getNettyClient().start();
        if (clientStatManager != null) {
            clientStatManager.start();
        }
        // 启动会话管理。
        sessionManager.start();
        // 启动长轮询
        longPullManager.start();
        // 启动重试
        //retryManager.start();

        // broker monitor
        brokerMonitor.start();
       // archiveManager.start();


        // 在start方法中初始化了集群数据
        clusterManager.start();

        // 启动角色选举
        roleDecider.start();
        // 给选举器添加监听器
        roleDecider.addListener(roleEventListener);
        //记录启动时间
        brokerMonitor.setBrokerStartTime(SystemClock.now());

        localOffsetManager.start();

//        if (roleDecider instanceof RaftRoleDecider) {
//            RaftRoleDecider raftRoleDecider = (RaftRoleDecider) roleDecider;
//            raftRoleDecider.setWaterMark(waterMark);
//        }

        logger.info("broker server is started");
    }

    @Override
    protected void startError(Exception e) {
        super.startError(e);
        logger.error("fail to start broker server");
    }

    @Override
    protected void beforeStop() {
        // 先通知集群和服务将要关闭，避免下面存储关闭后出现大量错误日志
        if (clusterManager != null) {
            clusterManager.willStop();
        }
        if (nettyServer != null) {
            nettyServer.willStop();
        }
        // 通知存储将要关闭，避免正在主从切换阻塞时间过程
        if (config != null && config.getStore() != null) {
            config.getStore().willStop();
        }
    }

    @Override
    protected void doStop() {
        // 加锁，防止启动和集群选举事件
        super.doStop();
        stopBrokerServices();
        NettyClient nettyClient = null;
        Store store = null;
        if (config != null) {
            nettyClient = config.getNettyClient();
            store = config.getStore();
            storeUnsafe = (StoreUnsafe) store;
            if (config.getRegistry() != null) {
                try {
                    config.getRegistry().stop();
                } catch (Exception ignored) {
                }
            }
        }

        if (nettyServer != null) {
            nettyServer.removeListener(transportEventListener);
        }
        if (contextManager != null) {
            contextManager.removeListener(config);
        }
//                .close(archiveManager).close(retryManager)
        Close.close(nettyServer).close(transactionManager)
                .close(longPullManager).close(dispatchService).close(putExecutor).close(getExecutor)
                .close(clusterManager).close(nettyClient).close(store).close(brokerMonitor).close(clientStatManager).close(scheduler).close(localOffsetManager);
        logger.info("broker server is stopped.");
    }

    /**
     * 启动兼容服务
     *
     * @throws Exception
     */
    protected void startBrokerServices() throws Exception {
        if (brokerServices != null) {
            for (BrokerService brokerService : brokerServices) {
                brokerService.start();
            }
        }
    }

    /**
     * 关闭兼容服务
     *
     * @throws Exception
     */
    protected void stopBrokerServices() {
        if (brokerServices != null) {
            for (BrokerService brokerService : brokerServices) {
                if (brokerService.isStarted()) {
                    brokerService.stop();
                }
            }
        }
    }

    /**
     * 注册中心监听器
     */
    protected class RegistryListener implements ConnectionListener {
        private CountDownLatch latch;

        public RegistryListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onEvent(ConnectionEvent event) {
            if (event.getType() == ConnectionEvent.ConnectionEventType.CONNECTED) {
                if (latch != null) {
                    latch.countDown();
                    latch = null;
                }
            }
        }
    }

    /**
     * 监听集群选举事件
     */
    protected class RoleEventListener implements EventListener<RoleEvent> {

        @Override
        public void onEvent(RoleEvent event) {
            logger.info("cluster info is changed to " + event.toString());
            // 得到集群状态
            ClusterRole role = event.getRole(broker);
            logger.info(String.format("broker %s ,current role is %s", broker.getAlias(), role));
            // 加锁，防止启动和关闭
            getWriteLock().lock();
            try {
                if (!isStarted()) {
                    return;
                }
                Broker lastMaster = event.getLastMaster();
                if (role == ClusterRole.SLAVE || role == ClusterRole.BACKUP) {
                    // 从节点
                    logger.info("lastMaster={}, newMaster={}", lastMaster, event.getMaster());
                    if (event.getMaster().equals(lastMaster)) {
                        // 主节点没有发生变化
                        return;
                    }
                    stopSlave();
                    if (broker.getRole() != role) {
                        broker.setRole(role);
                    }
                    startSlave(event.getMaster());
                } else if (role == ClusterRole.MASTER) {
                    if (replicationMaster != null && replicationMaster.isStarted()) {
                        // 当前节点已经是主节点
                        return;
                    }
                    // 原来是不是主节点，现在是主节点
                    // 停止从节点
                    stopSlave();

                    if (broker.getRole() != role) {
                        broker.setRole(role);
                    }
                    // 启动Master
                    startMaster();
                } else {
                    // 停止从节点
                    stopSlave();
                }
            } catch (Exception e) {
                // 启动服务出错，则停止服务
                stop();
                logger.error(e.getMessage(), e);
            } finally {
                getWriteLock().unlock();
            }
        }
    }

    protected void startMaster() throws Exception {
        replicationMaster.addListener(new EventListener<ReplicaEvent>() {
            @Override
            public void onEvent(ReplicaEvent replicaEvent) {
                if (replicaEvent.state == ReplicaEvent.State.esInsync || replicaEvent.state == ReplicaEvent.State.esSynchronizing) {
                    replicatedJournalOffset = replicaEvent.waterMark;
                    updateWaterMark(Math.min(replicatedJournalOffset, flushedJournalOffset));
                }
            }
        });
        replicationMaster.start();
    }

    protected void updateWaterMark(long waterMark) {
        long deltaTime = SystemClock.now() - waterMarkTimestamp;
        if ((logger.isDebugEnabled() && deltaTime >= 1000) || deltaTime >= 1000) {
            logger.info("JournalOffset: flushed={}, replicated={}", flushedJournalOffset, replicatedJournalOffset);
            waterMarkTimestamp = SystemClock.now();
        }
        if (waterMark > 0 && this.waterMark != waterMark) {
            this.waterMark = waterMark;
            storeUnsafe.updateWaterMark(waterMark);
            // signal flushWaterMarkThread to save waterMark to file
            flushWaterMarkFlag.set(true);
        }
    }

    protected void startSlave(Broker master) throws Exception {
        replicationSlave = new ReplicationSlaveService(master, broker, config.getReplicationConfig(), haListener, store, storeUnsafe);
//        if (roleDecider instanceof RaftRoleDecider) {
//            RaftRoleDecider raftRoleDecider = (RaftRoleDecider) roleDecider;
//            raftRoleDecider.setReplicationSlave(replicationSlave);
//        }
        // TODO: 2016/12/6 可能造成数据丢失
        if (waterMark > 0) {
            storeUnsafe.truncate(waterMark);
        }
        replicationSlave.start();
    }

    protected void stopSlave() {
        if (replicationSlave != null) {
            replicationSlave.stop();
            replicationSlave = null;
        }
    }

    /**
     * 监听Netty事件
     */
    protected class TransportEventListener implements EventListener<TransportEvent> {

        @Override
        public void onEvent(TransportEvent transportEvent) {
            if (transportEvent.getType() == TransportEvent.EventType.CLOSE || transportEvent.getType() ==
                    TransportEvent.EventType.EXCEPTION) {
                try {
                    Transport transport = transportEvent.getTransport();
                    Connection connection = (Connection) transport.attr(SessionManager.CONNECTION_KEY);
                    if (connection != null) {
                        logger.info(String.format("connection is closed. id:%s,app:%s,ip:%s.", connection.getId(),
                                connection.getApp(), Ipv4.toAddress(transport.remoteAddress())));

                        sessionManager.removeConnection(connection.getId());
                    }
                } catch (Exception e) {
                    logger.error("delete connection failed.", e);
                }
            }

        }
    }

    /**
     * 存储服务
     */
    protected class HAListener implements ReplicationListener {

        @Override
        public void onStart() throws Exception {
            // 在角色转换过程中启动Netty
            try {
                dispatchService.start();
                transactionManager.start();
                nettyServer.start();
                startBrokerServices();
                logger.info(String.format("broker server is listening on %s:%d", broker.getIp(), broker.getPort()));
            } catch (Exception e) {
                logger.error("start broker service error!", e);
            }
        }

        @Override
        public void onStop() {
            Close.close(nettyServer).close(transactionManager).close(dispatchService);
            stopBrokerServices();
        }

        @Override
        public void onStart(ClusterRole role) throws Exception {

        }

        @Override
        public void onAddReplica(Replica replica) {

        }

        @Override
        public void onRemoveReplica(Replica replica) {

        }
    }
}