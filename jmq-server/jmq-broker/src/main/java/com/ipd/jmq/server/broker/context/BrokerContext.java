package com.ipd.jmq.server.broker.context;

import com.ipd.jmq.server.context.ContextEvent;

/**
 * broker 上下文.
 *
 * @author lindeqiang
 * @since 2016/8/19 14:03
 */
public class BrokerContext extends ContextEvent {

    public static final String BROKER_GROUP = "broker";
    public static final String HA_GROUP = "ha";
    public static final String RETRY_GROUP = "retry";
    public static final String RETRY_SERVER_GROUP = "rs";
    public static final String NETTY_SERVER_GROUP = "server";
    public static final String NETTY_CLIENT_GROUP = "client";
    public static final String STORE_GROUP = "store";
    public static final String ARCHIVE_GROUP = "archive";
    public static final String CONTROL_GROUP = "control";

    public static final String RETRY_MAX_RETRYS = "retry.maxRetrys";
    public static final String RETRY_MAX_RETRY_DELAY = "retry.maxRetryDelay";
    public static final String RETRY_RETRY_DELAY = "retry.retryDelay";
    public static final String RETRY_USE_EXPONENTIAL_BACK_OFF = "retry.useExponentialBackOff";
    public static final String RETRY_BACK_OFF_MULTIPLIER = "retry.backOffMultiplier";
    public static final String RETRY_EXPIRE_TIME = "retry.expireTime";



    public static final String BROKER_CLIENT_PROFILE_INTERVAL = "broker.clientProfileInterval";
    public static final String BROKER_CLIENT_MACHINE_METRICS_INTERVAL = "broker.clientMachineMetricsInterval";
    public static final String BROKER_CLIENT_TP_FLAG = "broker.clientTpFlag";
    public static final String BROKER_CLIENT_METRICS_FLAG = "broker.clientMetricsFlag";
    public static final String BROKER_CLIENT_LOAD_BALANCE_OPENER = "broker.clientLoadBalanceOpener";
    public static final String BROKER_UPDATE_CLUSTER_INTERVAL = "broker.updateClusterInterval";
    public static final String BROKER_ADMIN_USER = "broker.adminUser";
    public static final String BROKER_ADMIN_PASSWORD = "broker.adminPassword";
    public static final String BROKER_ROLE_DECIDER = "broker.roleDecider";
    public static final String BROKER_GET_THREADS = "broker.getThreads";
    public static final String BROKER_GET_QUEUE_CAPACITY = "broker.getQueueCapacity";
    public static final String BROKER_PUT_THREADS = "broker.putThreads";
    public static final String BROKER_PUT_QUEUE_CAPACITY = "broker.putQueueCapacity";
    public static final String BROKER_ADMIN_THREADS = "broker.adminThreads";
    public static final String BROKER_ACK_TIMEOUT = "broker.ackTimeout";
    public static final String BROKER_BATCH_SIZE = "broker.batchSize";
    public static final String BROKER_MAX_LONG_PULLS = "broker.maxLongPulls";
    public static final String BROKER_CHECK_ACK_EXPIRE_INTERVAL = "broker.checkAckExpireInterval";
    public static final String BROKER_CHECK_TRANSACTION_EXPIRE_INTERVAL = "broker.checkTransactionExpireInterval";
    public static final String BROKER_PERF_STAT_INTERVAL = "broker.perfStatInterval";
    public static final String BROKER_CONSUMER_CONTINUE_ERR_PAUSE_INTERVAL = "broker.consumerContinueErrPauseInterval";
    public static final String BROKER_CONSUMER_CONTINUE_ERR_PAUSE_TIMES = "broker.consumerContinueErrPauseTimes";
    public static final String BROKER_CONSUMER_REBALANCE_INTERVAL = "broker.consumerReBalanceInterval";
    public static final String BROKER_CLUSTER_BODY_CACHE_TIME = "broker.clusterBodyCacheTimeInIDC";
    public static final String BROKER_CLUSTER_LARGEBODY_CACHE_TIME = "broker.clusterLargeBodyCacheTimeInIDC";
    public static final String BROKER_CLUSTER_LARGEBODY_SIZE_THRESHOLD = "broker.clusterLargeBodySizeThresholdInIDC";

    public static final String SERVER_SEND_TIMEOUT = "server.sendTimeout";
    public static final String SERVER_WORKER_THREADS = "server.workerThreads";
    public static final String SERVER_CALLBACK_EXECUTOR_THREADS = "server.callbackExecutorThreads";
    public static final String SERVER_SELECTOR_THREADS = "server.selectorThreads";
    public static final String SERVER_CHANNEL_MAX_IDLE_TIME = "server.channelMaxIdleTime";
    public static final String SERVER_SO_TIMEOUT = "server.soTimeout";
    public static final String SERVER_SOCKET_BUFFER_SIZE = "server.socketBufferSize";
    public static final String SERVER_MAX_ONEWAY = "server.maxOneway";
    public static final String SERVER_MAX_ASYNC = "server.maxAsync";
    public static final String SERVER_BACKLOG = "server.backlog";

    public static final String CLIENT_SEND_TIMEOUT = "client.sendTimeout";
    public static final String CLIENT_WORKER_THREADS = "client.workerThreads";
    public static final String CLIENT_CALLBACK_EXECUTOR_THREADS = "client.callbackExecutorThreads";
    public static final String CLIENT_SELECTOR_THREADS = "client.selectorThreads";
    public static final String CLIENT_CHANNEL_MAX_IDLE_TIME = "client.channelMaxIdleTime";
    public static final String CLIENT_SO_TIMEOUT = "client.soTimeout";
    public static final String CLIENT_SOCKET_BUFFER_SIZE = "client.socketBufferSize";
    public static final String CLIENT_MAX_ONEWAY = "client.maxOneway";
    public static final String CLIENT_MAX_ASYNC = "client.maxAsync";
    public static final String CLIENT_CONNECTION_TIMEOUT = "client.connectionTimeout";

    public static final String RETRY_SERVER_QUEUE_SIZE = "rs.queueSize";
    public static final String RETRY_SERVER_FETCH_SIZE = "rs.fetchSize";
    public static final String RETRY_SERVER_FETCH_THREADS = "rs.fetchThreads";
    public static final String RETRY_SERVER_FETCH_INTERVAL = "rs.fetchInterval";
    public static final String RETRY_SERVER_CLEAR_INTERVAL = "rs.clearInterval";
    public static final String RETRY_SERVER_TIMEOUT = "rs.timeout";
    public static final String RETRY_SERVER_STAT_TIME_WINDOW_SIZE = "rs.statTimeWindowSize";
    public static final String RETRY_SERVER_LIMIT_TIMES = "rs.limitTimes";
    public static final String RETRY_SERVER_LIMIT_SPEED = "rs.limitSpeed";
    public static final String RETRY_REMOTE_LOAD_RETRY_COUNT = "rs.remoteLoadRetryCount";
    public static final String RETRY_REMOTE_RETRY_LIMIT_THREAD = "rs.remoteRetryLimitThread";


    public static final String ARCHIVE_PRODUCE_THRESHOLD = "archive.produceThreshold";
    public static final String ARCHIVE_CAPACITY = "archive.capacity";
    public static final String ARCHIVE_CONSUME_THRESHOLD = "archive.consumeThreshold";
    public static final String ARCHIVE_PRODUCE_FILE_SIZE = "archive.produceFileSize";
    public static final String ARCHIVE_CONSUME_FILE_SIZE = "archive.consumeFileSize";
    public static final String ARCHIVE_ROLLING_INTERVAL = "archive.rollingInterval";
    public static final String ARCHIVE_PRODUCE_ENQUEUE_TIMEOUT = "archive.produceEnqueueTimeout";
    public static final String ARCHIVE_CONSUME_ENQUEUE_TIMEOUT = "archive.consumeEnqueueTimeout";



    public static final String BROADCAST_OFFSET_ACK_TIME_DIFFERENCE = "broadcast.OffsetAckTimeDifference";
    public static final String BROADCAST_OFFSET_ACK_INTERVAL = "broadcast.OffsetAckInterval";
    public static final String BROKER_TOPIC_CONTROLLER_PERMISSION = "control.topicBrokerPubSub";
    public static final String BROKER_APP_CONTROLLER_CLIENTIP = "control.clientIpPubSub";
    public static final String DELETE_OLD_TOPIC_CONTROLLER_DATA = "control.deleteOldTopicControllerData";
    public static final String DELETE_OLD_APP_CONTROLLER_DATA = "control.deleteOldAppControllerData";
    public static final String CLEAR_ALL_CONTROLLER_DATA = "control.clearAllControllerData";



    public static final String KAFKA_TOPIC_METADATA_STAT_TIME_WINDOW_SIZE = "kafka.topicMetadataStatTimeWindowSize";
    public static final String KAFKA_TOPIC_METADATA_LIMIT_TIMES = "kafka.topicMetadataLimitTimes";


    public BrokerContext(String key, String value) {
        super(key, value);
    }
}
