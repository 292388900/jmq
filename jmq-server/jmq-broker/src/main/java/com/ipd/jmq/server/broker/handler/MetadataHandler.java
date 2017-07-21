package com.ipd.jmq.server.broker.handler;

import com.alibaba.fastjson.JSON;
import com.ipd.jmq.common.cluster.BrokerCluster;
import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.cluster.DataCenter;
import com.ipd.jmq.common.model.DynamicClientConfig;
import com.ipd.jmq.common.network.v3.codec.encode.GetClusterAckEncoder;
import com.ipd.jmq.common.network.v3.command.*;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.SessionManager;
import com.ipd.jmq.server.broker.cluster.ClusterEvent;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.broker.monitor.BrokerMonitor;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.lang.Charsets;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.network.Ipv4;
import com.ipd.jmq.toolkit.time.SystemClock;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;


/**
 * 配置处理器
 */
public class MetadataHandler extends AbstractHandler implements JMQHandler {
    private static final Logger logger = LoggerFactory.getLogger(MetadataHandler.class);
    private ClusterManager clusterManager;
    private BrokerConfig config;
    private ConcurrentHashMap<String, TimeWrapperObj> cacheClusterBody = new ConcurrentHashMap<String, TimeWrapperObj>();
    private ConcurrentHashMap<String, Boolean> calculatingAppSet = new ConcurrentHashMap<String, Boolean>();
    private ThreadPoolExecutor calcClusterLargeBodyThreadPool = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS, new ArrayBlockingQueue(1000));
    private GetClusterAckEncoder encoder = new GetClusterAckEncoder();
    public MetadataHandler(){}
    public MetadataHandler(final ExecutorService executorService, final SessionManager sessionManager, final ClusterManager
            clusterManager, final BrokerConfig config, final BrokerMonitor brokerMonitor) {
        Preconditions.checkArgument(config != null, "config can not be null");
        Preconditions.checkArgument(brokerMonitor != null, "brokerMonitor can not be null");
        Preconditions.checkArgument(clusterManager != null, "clusterManager can not be null");
        Preconditions.checkArgument(sessionManager != null, "sessionManager can not be null");
        Preconditions.checkArgument(executorService != null, "executorService can not be null");
//        Preconditions.checkArgument(config.getCacheService() != null, "cacheService can not be null");

        this.executorService = executorService;
        this.sessionManager = sessionManager;
        this.broker = clusterManager.getBroker();
        this.clusterManager = clusterManager;
        this.config = config;

        clusterManager.addListener(new EventListener<ClusterEvent>() {
            @Override
            public void onEvent(ClusterEvent event) {
                switch (event.getType()) {
                    case ALL_BROKER_UPDATE:
                    case ALL_TOPIC_UPDATE:
                    case ALL_SLAVECONSUME_UPDATE:
                        int calcCount = 0;
                        for (String key : cacheClusterBody.keySet()) {
                            TimeWrapperObj obj = cacheClusterBody.get(key);
                            if (obj == null || ((byte[]) obj.getObj()).length < config.getClusterLargeBodySizeThreshold())
                                continue;
                            int dataCenterSplitIndex = key.indexOf(':');
                            if (dataCenterSplitIndex < 0)
                                continue;
                            long dataCenterId = Long.parseLong(key.substring(0, dataCenterSplitIndex));
                            String app = key.substring(dataCenterSplitIndex + 1, key.indexOf(':', dataCenterSplitIndex + 1));
                            if (!addCalcClusterBodyTask(dataCenterId, app, key, new GetClusterAck(), true))
                                break;
                            ++calcCount;
                        }
                        logger.info("on event {} recalculate cacheClusterBody (total={}, calc={})",
                                event.getType() == ClusterEvent.EventType.ALL_BROKER_UPDATE ? "ALL_BROKER_UPDATE" : "ALL_TOPIC_UPDATE", cacheClusterBody.size(), calcCount);
                        break;
                }
            }
        });
    }
    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }
    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }
    public void setClusterManager(ClusterManager clusterManager) {
        this.broker = clusterManager.getBroker();
        this.clusterManager = clusterManager;
        clusterManager.addListener(new EventListener<ClusterEvent>() {
            @Override
            public void onEvent(ClusterEvent event) {
                switch (event.getType()) {
                    case ALL_BROKER_UPDATE:
                    case ALL_TOPIC_UPDATE:
                    case ALL_SLAVECONSUME_UPDATE:
                        int calcCount = 0;
                        for (String key : cacheClusterBody.keySet()) {
                            TimeWrapperObj obj = cacheClusterBody.get(key);
                            if (obj == null || ((byte[]) obj.getObj()).length < config.getClusterLargeBodySizeThreshold())
                                continue;
                            int dataCenterSplitIndex = key.indexOf(':');
                            if (dataCenterSplitIndex < 0)
                                continue;
                            long dataCenterId = Long.parseLong(key.substring(0, dataCenterSplitIndex));
                            String app = key.substring(dataCenterSplitIndex + 1, key.indexOf(':', dataCenterSplitIndex + 1));
                            if (!addCalcClusterBodyTask(dataCenterId, app, key, new GetClusterAck(), true))
                                break;
                            ++calcCount;
                        }
                        logger.info("on event {} recalculate cacheClusterBody (total={}, calc={})",
                                event.getType() == ClusterEvent.EventType.ALL_BROKER_UPDATE ? "ALL_BROKER_UPDATE" : "ALL_TOPIC_UPDATE", cacheClusterBody.size(), calcCount);
                        break;
                }
            }
        });
    }

    public void setConfig(BrokerConfig config) {
        this.config = config;
    }

    private boolean addCalcClusterBodyTask(final long dataCenterId, final String app, final String key, final GetClusterAck getClusterAck, final boolean flush) {
        // 如果没有计算这个app的任务，则在另一线程池计算新的ClusterBody
        if (!flush && calculatingAppSet.containsKey(key)) {
            return true;
        }
        try {
            calculatingAppSet.put(key, true);
            calcClusterLargeBodyThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        long timeStart = SystemClock.now();
                        byte[] body = calcClusterBody(dataCenterId, app, key, getClusterAck, key.split(":")[3]);
                        int duration = (int) (SystemClock.now() - timeStart);
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("getClusterLargeBodySize: key=%s, bodySize=%d, duration=%d",
                                    key, body.length, duration));
                        }
                    } catch (Exception e) {
                        logger.error("calcClusterBody error,app=" + app, e);
                    } finally {
                        calculatingAppSet.remove(key);
                    }
                }
            });
            return true;
        } catch (RejectedExecutionException e) {
            calculatingAppSet.remove(key);
            logger.warn("calcClusterLargeBodyThreadPool exceeded queue size {}", calcClusterLargeBodyThreadPool.getQueue().size());
        }
        return false;
    }

    private byte[] calcClusterBody(final long dataCenterId, final String app, final String key,
                                   final GetClusterAck getClusterAck, final String clientId) throws Exception {
        long timeStart = SystemClock.now();
        final List<BrokerCluster> clusters = clusterManager.getClusters(app, dataCenterId, clientId);

        //过滤顺序消息的权限
        try {
            clusterManager.checkSequentialCluster(clusters, app);
        } catch (Exception e) {
            logger.error("Check sequential state error!", e);
        }

        TopicConfig topicConfig;
        ConcurrentMap<String, TopicConfig> topicConfigs = new ConcurrentHashMap<String, TopicConfig>();
        for (BrokerCluster brokerCluster : clusters) {
            topicConfig = clusterManager.getTopicConfig(brokerCluster.getTopic());
            topicConfigs.put(brokerCluster.getTopic(), topicConfig);
        }
        String topicConfigStr = JSON.toJSONString(topicConfigs);

        getClusterAck.clusters(clusters).interval(config.getUpdateClusterInterval())
                .maxSize(config.getStoreConfig().getMaxMessageSize()).allTopicConfigStrings(topicConfigStr);

        // 创建buffer
        ByteBuf byteBuf = Unpooled.buffer();
        // 调用encode
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("getTopicConfig: key=%s, duration=%d",
                    key, SystemClock.now() - timeStart));
        }

        // set identity before encoding
        byte identity = 1;
        if (cacheClusterBody.get(key) != null) {
            identity = cacheClusterBody.get(key).getIdentity();
            identity += 1;
        }
        getClusterAck.identity(identity);

        // 编码获取集群应答
        encoder.encode(getClusterAck, byteBuf);
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("encodeBody: key=%s, bodySize=%d, duration=%d",
                    key, byteBuf.readableBytes(), SystemClock.now() - timeStart));
        }
        byte[] body = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(body);

        // 将body缓存
        if (config.getClusterBodyCacheTime() > 0) {
            if (body.length < config.getClusterLargeBodySizeThreshold()) {
                cacheClusterBody.put(key, new TimeWrapperObj(body, config.getClusterBodyCacheTime(), identity));
            } else {
                cacheClusterBody.put(key, new TimeWrapperObj(body, config.getClusterLargeBodyCacheTime(), identity));
            }
        }

        return body;

    }

    @Override
    public int[] type() {
        return new int[]{CmdTypes.GET_CLUSTER};
    }

    @Override
    public Command process(Transport transport, Command command) throws TransportException {
        JMQHeader header = (JMQHeader) command.getHeader();
        GetCluster payload = (GetCluster) command.getPayload();
        try {
            final GetClusterAck getClusterAck = new GetClusterAck();
            byte version = header.getVersion();

            ConcurrentMap<String, DynamicClientConfig> changedClientConfig = clusterManager.getDynamicClientConfigs();
            if (null != changedClientConfig && !changedClientConfig.isEmpty()) {
                getClusterAck.setClientConfigs(JSON.toJSONString(changedClientConfig));
            }

            final String app = payload.getApp();
            long dataCenterId = -1L;
            if (payload.getDataCenter() <= 0) {
                // 获取数据中心
                String clientIp = Ipv4.toAddress(transport.remoteAddress());

                DataCenter dataCenter = clusterManager.getDataCenter(clientIp);
                if (dataCenter != null) {
                    getClusterAck.setDataCenter((byte) dataCenter.getId());
                    dataCenterId = dataCenter.getId();
                }
            } else {
                dataCenterId = payload.getDataCenter();
            }

            final String key = String.format("%d:%s:%s:%s", dataCenterId, app, version < 3 ? 2 : 3, payload.getClientId());
            TimeWrapperObj obj = cacheClusterBody.get(key);

            if (config.getClusterBodyCacheTime() > 0 && obj != null && obj.getTimeStamp() > SystemClock.now()) {
                if (obj.getIdentity() != payload.getIdentity()) {
                    getClusterAck.setCacheBody((byte[]) obj.getObj());
                }
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("app=%s,use cache!", app));
                }
            } else {
                // 如果没有缓存ClusterBody或ClusterBody小于阀值则在本线程重新计算
                if (config.getClusterBodyCacheTime() <= 0 || obj == null || ((byte[]) obj.getObj()).length < config.getClusterLargeBodySizeThreshold()) {
                    byte[] body = calcClusterBody(dataCenterId, app, key, getClusterAck, payload.getClientId());
                    if (payload.getIdentity() != cacheClusterBody.get(key).getIdentity()) {
                        getClusterAck.setCacheBody(body);
                    }

                } else {
                    // 返回旧的ClusterBody，减少等待时间
                    if (obj.getIdentity() != payload.getIdentity()) {
                        getClusterAck.setCacheBody((byte[]) obj.getObj());
                    }

                    //超时两倍则强制刷新，防止map中key没有清理等原因一直不更新。
                    GetClusterAck newClusterAckForCalc = new GetClusterAck();
                    addCalcClusterBodyTask(dataCenterId, app, key, newClusterAckForCalc, obj.getTimeStamp() + config.getClusterBodyCacheTime() < SystemClock.now());
                }

                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("app=%s,did not use cache!", app));
                }
            }

            return new Command(JMQHeader.Builder.create().direction(Direction.RESPONSE).type(getClusterAck.type()).
                    requestId(header.getRequestId()).status(JMQCode.SUCCESS.getCode()).
                    error(JMQCode.SUCCESS.getMessage()).build(),
                    getClusterAck);
        } catch (JMQException e) {
            throw new TransportException(e.getMessage(), e.getCause(), e.getCode());
        } catch (Throwable e) {
            logger.error(String.format("getCluster error.payload=[%s],clientIP=%s", payload, Ipv4.toAddress(transport.remoteAddress())), e);
            throw new TransportException(e.getMessage(), e.getCause(), JMQCode.CN_UNKNOWN_ERROR.getCode());
        }
    }

    private class TimeWrapperObj {
        private long timeStamp;
        private Object obj;
        private byte identity = 1;

        public TimeWrapperObj(Object obj, long deltaTime, byte identity) {
            this.obj = obj;
            timeStamp = SystemClock.now() + deltaTime;
            this.identity = identity;
        }

        public long getTimeStamp() {
            return timeStamp;
        }

        public void setTimeStamp(long timeStamp) {
            this.timeStamp = timeStamp;
        }

        public Object getObj() {
            return obj;
        }

        public void setObj(Object obj) {
            this.obj = obj;
        }

        public byte getIdentity() {
            return identity;
        }

        public void setIdentity(byte identity) {
            this.identity = identity;
        }

        @Override
        public String toString() {
            String objString = new String((byte[]) obj, Charsets.UTF_8);
            return "TimeWrapperObj{" +
                    "timeStamp=" + timeStamp +
                    ", obj=" + objString +
                    ", identity=" + identity +
                    '}';
        }
    }

}