package com.ipd.jmq.server.broker.service;

import com.ipd.jmq.common.network.v3.command.GetMessage;
import com.ipd.jmq.common.network.v3.command.PutMessage;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.SessionManager;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
import com.ipd.jmq.server.broker.election.RoleDecider;
import com.ipd.jmq.server.broker.handler.DefaultHandlerFactory;
import com.ipd.jmq.server.broker.handler.GetMessageHandler;
import com.ipd.jmq.server.broker.handler.PutMessageHandler;
import com.ipd.jmq.server.store.Store;
import com.ipd.kafka.DelayedResponseManager;
import com.ipd.kafka.cluster.KafkaClusterManager;
import com.ipd.kafka.coordinator.GroupCoordinator;
import com.ipd.kafka.handler.*;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.toolkit.URL;
import com.ipd.jmq.toolkit.concurrent.NamedThreadFactory;
import com.ipd.jmq.toolkit.concurrent.Scheduler;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.service.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhangkepeng on 16-12-14.
 */
public class KafkaBrokerService extends Service implements BrokerService {

    private BrokerConfig brokerConfig;
    private Store store;
    private ClusterManager clusterManager;
    private SessionManager sessionManager;
    private Scheduler scheduler;
    private RoleDecider roleDecider;
    private DispatchService dispatchService;
    private DefaultHandlerFactory factory;
    private KafkaClusterManager kafkaClusterManager;
    private GroupCoordinator coordinator;

    private ProduceHandler produceHandler;
    private FetchHandler fetchHandler;
    private OffsetHandler offsetHandler;
    private TopicMetadataHandler topicMetadataHandler;
    private OffsetFetchHandler offsetFetchHandler;
    private OffsetCommitHandler offsetCommitHandler;
    private UpdateTopicsBrokerHandler updateTopicsBrokerHandler;
    private GroupCoordinatorHandler groupCoordinatorHandler;
    private JoinGroupHandler joinGroupHandler;
    private HeartbeatHandler heartbeatHandler;
    private SyncGroupHandler syncGroupHandler;
    private LeaveGroupHandler leaveGroupHandler;
    private OffsetQueryHandler offsetQueryHandler;

    // 消费消息线程池
    protected ExecutorService executor;
    protected DelayedResponseManager delayedResponseManager;
    private URL url;

    @Override
    protected void validate() throws Exception {
        super.validate();
        Preconditions.checkArgument(brokerConfig != null, "BrokerConfig can not be null");
        Preconditions.checkArgument(store != null, "Store can not be null");
        Preconditions.checkArgument(clusterManager != null, "ClusterManager can not be null");
        Preconditions.checkArgument(sessionManager != null, "SessionManager can not be null");
        Preconditions.checkArgument(scheduler != null, "Scheduler can not be null");
        Preconditions.checkArgument(roleDecider != null, "RoleDecider can not be null");
        Preconditions.checkArgument(dispatchService != null, "DispatchService can not be null");
        Preconditions.checkArgument(factory != null, "AbstractHandlerFactory can not be null");

        // 获取JMQ handler
        PutMessage putMessage = new PutMessage();
        GetMessage getMessage = new GetMessage();
        Command command = new Command();
        command.setPayload(putMessage);
        PutMessageHandler putMessageHandler = (PutMessageHandler) factory.getHandler(command);
        command.setPayload(getMessage);
        GetMessageHandler getMessageHandler = (GetMessageHandler) factory.getHandler(command);
        // 创建kafka集群管理
        if (kafkaClusterManager == null) {
            kafkaClusterManager = new KafkaClusterManager(roleDecider, clusterManager, brokerConfig);
        }
        if (produceHandler == null) {
            produceHandler = new ProduceHandler(kafkaClusterManager, putMessageHandler, brokerConfig);
        }
        if (executor == null) {
            executor = new ThreadPoolExecutor(brokerConfig.getGetThreads(), brokerConfig.getGetThreads(), 1000 * 60,
                    TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(brokerConfig.getGetQueueCapacity()),
                    new NamedThreadFactory("KAFKA_PULL_EXECUTOR"));
        }
        if (coordinator == null) {
            coordinator = new GroupCoordinator(brokerConfig, kafkaClusterManager, dispatchService);
        }
        if (delayedResponseManager == null) {
            delayedResponseManager = new DelayedResponseManager(getMessageHandler, executor, brokerConfig);
        }
        if (fetchHandler == null) {
            fetchHandler = new FetchHandler(getMessageHandler, delayedResponseManager);
        }
        if (offsetHandler == null) {
            offsetHandler = new OffsetHandler(delayedResponseManager, coordinator);
        }
        if (topicMetadataHandler == null) {
            topicMetadataHandler = new TopicMetadataHandler(delayedResponseManager, kafkaClusterManager, brokerConfig, scheduler);
        }
        if (offsetFetchHandler == null) {
            offsetFetchHandler = new OffsetFetchHandler(coordinator);
        }
        if (offsetCommitHandler == null) {
            offsetCommitHandler = new OffsetCommitHandler(coordinator);
        }
        if (updateTopicsBrokerHandler == null) {
            updateTopicsBrokerHandler = new UpdateTopicsBrokerHandler(kafkaClusterManager);
        }
        if (groupCoordinatorHandler == null) {
            groupCoordinatorHandler = new GroupCoordinatorHandler(delayedResponseManager, kafkaClusterManager);
        }
        if (joinGroupHandler == null) {
            joinGroupHandler = new JoinGroupHandler(coordinator);
        }
        if (heartbeatHandler == null) {
            heartbeatHandler = new HeartbeatHandler(coordinator);
        }
        if (syncGroupHandler == null) {
            syncGroupHandler = new SyncGroupHandler(coordinator);
        }
        if (leaveGroupHandler == null) {
            leaveGroupHandler = new LeaveGroupHandler(coordinator);
        }
        if (offsetQueryHandler == null) {
            offsetQueryHandler = new OffsetQueryHandler(kafkaClusterManager, dispatchService);
        }
    }

    @Override
    public void doStart() throws Exception {
        super.doStart();
        factory.register(produceHandler).register(fetchHandler).register(offsetHandler).
                register(topicMetadataHandler).register(offsetFetchHandler).register(offsetCommitHandler).
                register(updateTopicsBrokerHandler).register(groupCoordinatorHandler).register(joinGroupHandler).
                register(heartbeatHandler).register(syncGroupHandler).register(leaveGroupHandler).register(offsetQueryHandler);
        delayedResponseManager.start();
        kafkaClusterManager.start();
        coordinator.start();
    }

    @Override
    protected void doStop() {
        super.stop();
        delayedResponseManager.stop();
        kafkaClusterManager.stop();
        coordinator.stop();
    }

    @Override
    public void setBrokerConfig(BrokerConfig config) {
        this.brokerConfig = config;
    }

    @Override
    public void setStore(Store store) {
        this.store = store;
    }

    @Override
    public void setClusterManager(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    @Override
    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public void setDispatchService(DispatchService dispatchService) {
        this.dispatchService = dispatchService;
    }

    @Override
    public void setRoleDecider(RoleDecider roleDecider) {
        this.roleDecider = roleDecider;
    }

    @Override
    public void setCommandHandlerFactory(DefaultHandlerFactory factory) {
        this.factory = factory;
    }

    @Override
    public String getType() {
        return "kafka";
    }

    /**
     * 设置URL
     *
     * @param url
     */
    @Override
    public void setUrl(URL url) {
        this.url = url;
    }

}
