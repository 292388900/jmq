package com.ipd.jmq.client.cluster;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ipd.jmq.client.exception.ExceptionHandler;
import com.ipd.jmq.common.model.ConsumerConfig;
import com.ipd.jmq.common.model.DynamicClientConfig;
import com.ipd.jmq.common.model.ProducerConfig;
import com.ipd.jmq.common.network.v3.command.JMQHeader;
import com.ipd.jmq.client.cluster.ClusterEvent.EventType;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.common.cluster.*;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.network.v3.command.GetCluster;
import com.ipd.jmq.common.network.v3.command.GetClusterAck;
import com.ipd.jmq.common.network.v3.netty.NettyClient;
import com.ipd.jmq.common.network.v3.session.ClientId;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.model.Acknowledge;
import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.toolkit.concurrent.EventBus;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.network.Ipv4;
import com.ipd.jmq.toolkit.retry.RetryPolicy;
import com.ipd.jmq.toolkit.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 集群管理
 *
 * @author lindeqiang
 * @since 14-4-29 上午10:37
 */
public class ClusterManager extends Service {
    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);
    // 数据中心
    protected byte dataCenter;
    // 集群
    protected ConcurrentMap<String, BrokerCluster> clusters = new ConcurrentHashMap<String, BrokerCluster>();
    // 主题配置
    protected ConcurrentMap<String, TopicConfig> topicConfigs = new ConcurrentHashMap<String, TopicConfig>();
    // client configs
    private ConcurrentMap<String, DynamicClientConfig> clientConfigs = new ConcurrentHashMap<String, DynamicClientConfig>();
    // 主题配置字符串
    protected String allTopicConfigStrings;
    // 文件
    protected File file;
    // 集群事件管理器
    protected EventBus<ClusterEvent> eventManager = new EventBus<ClusterEvent>("ClusterManager");
    // 集群更新管理器
    protected EventBus<ClusterEvent.EventType> clusterUpdateTasks;
    // 更新集群时间间隔(毫秒)
    protected volatile int updateClusterInterval = 30000;
    // 更新集群超时时间(毫秒)
    protected int updateClusterTimeout = 15 * 1000;
    // 单次调用，生产消息的消息体合计最大大小(字节)
    protected volatile int maxSize = 1024 * 1024 * 4;
    // netty 客户端
    protected NettyClient client;
    // 客户端ID
    protected ClientId clientId;
    // 更新集群线程
    protected Thread updateClusterThread;
    // 连接配置
    protected TransportConfig config;
    // 更新集群监听器
    protected UpdateClusterListener updateClusterListener = new UpdateClusterListener();
    // sequential topic address
    protected List<String> sequentialAddress;
    // identity for changes
    private byte identity = 0;

    public ClusterManager(NettyClient client, ClientId clientId, File file) {
        if (client == null) {
            throw new IllegalArgumentException("client can not be null");
        }
        if (clientId == null) {
            throw new IllegalArgumentException("clientId can not be null");
        }
        this.client = client;
        this.clientId = clientId;
        this.file = file;
        this.config = (TransportConfig) client.getConfig();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        eventManager.start();
        logger.info("try to update cluster.");
        boolean success = false;
        // 首先尝试从服务器更新集群
        List<String> addresses = config.getAddresses();
        if (addresses != null && !addresses.isEmpty()) {
            // 复制一份
            addresses = new ArrayList<String>(addresses);
            int size = addresses.size();
            for (int i = 0; i < size; i++) {
                // 随机选择一份
                int pos = (int) (Math.random() * addresses.size());
                String address = addresses.remove(pos);
                if (updateCluster(address)) {
                    success = true;
                    logger.info("success updating cluster from " + address);
                    break;
                }
            }
        }
        // 没有成功，则尝试从本地文件加载
        if (!success && file != null && file.exists()) {
            logger.warn("update cluster from server error,servers:"+(addresses != null?addresses.toString():""+",prepare get tmp file"));
            load(file);
            // 广播事件
            if (!clusters.isEmpty() && !eventManager.getListeners().isEmpty()) {
                for (BrokerCluster cluster : clusters.values()) {
                    for (BrokerGroup group : cluster.getGroups()) {
                        if (group.getPermission() != Permission.NONE) {
                            eventManager.add(new ClusterEvent(EventType.ADD_BROKER, cluster.getTopic(), group,
                                    cluster.getQueues()));
                        }
                    }
                    // 最后广播队列数变更
                    eventManager.add(new ClusterEvent(EventType.QUEUE_CHANGE, cluster.getTopic(), cluster.getQueues()));
                }
                logger.info("success loading cluster from file " + file.getPath());
            }
        }

        // 启动定时更新线程
        clusterUpdateTasks = new EventBus<ClusterEvent.EventType>("ClusterUpdater") {
            @Override
            protected void publish(List<Ownership> events) {
                // 合并事件
                if (events == null || events.isEmpty()) {
                    return;
                }
                publish(events.get(0));
            }

            @Override
            protected void onIdle() {
                if (isStarted()) {
                    clusterUpdateTasks.add(ClusterEvent.EventType.GET_CLUSTER);
                }
            }
        };

        clusterUpdateTasks.start();
        //空闲时触发onIdle
        clusterUpdateTasks.setIdleTime(updateClusterInterval);
        //合并3秒钟的事件
        clusterUpdateTasks.setInterval(3000L);
        //添加监听器
        clusterUpdateTasks.addListener(updateClusterListener);
        logger.info("cluster manager is started");
    }

    @Override
    protected void doStop() {
        if (updateClusterThread != null) {
            updateClusterThread.interrupt();
            updateClusterThread = null;
        }
        if (file != null) {
            save(file);
        }
        clusters.clear();
        eventManager.stop();

        super.doStop();
        logger.info("cluster manager is stopped");
    }

    public byte getDataCenter() {
        return dataCenter;
    }

    public int getMaxSize() {
        return maxSize;
    }

    /**
     * 更新集群信息
     */
    protected boolean updateCluster() {
        // 先尝试从顺序消息集群里取地址
        String address = null;
        List<String> seqAddresses = sequentialAddress;
        if (seqAddresses != null && seqAddresses.size() > 0) {
            address = seqAddresses.remove(0);
        }

        // 如果地址为空，则从正常的地址当中选
        if (address == null) {
            List<String> addresses = config.getAddresses();
            if (addresses == null || addresses.isEmpty()) {
                return false;
            }
            int pos = (int) (Math.random() * addresses.size());
            address = addresses.get(pos);
        }
        return updateCluster(address);
    }

    /**
     * 更新集群信息
     *
     * @param address 地址
     */
    synchronized protected boolean updateCluster(String address) {
        Transport transport = null;
        try {
            // 创建连接
            InetSocketAddress socketAddress = config.getSocketAddress(address);
            if (socketAddress == null) {
                return false;
            }
            transport = client.createTransport(socketAddress);
            // 发送获取集群命令
            GetCluster getCluster = new GetCluster()
                    .app(config.getApp())
                    .clientId(clientId.getClientId())
                    .dataCenter(dataCenter)
                    .identity(identity);
            Command request = new Command(JMQHeader.Builder.request(
                    getCluster.type(), Acknowledge.ACK_RECEIVE), getCluster);
            Command response = transport.sync(request, this.updateClusterTimeout);
            // 判断是否成功
            JMQHeader header = (JMQHeader) response.getHeader();
            if (header.getStatus() != JMQCode.SUCCESS.getCode()) {
                throw new JMQException(header.getError(), header.getStatus());
            }

            GetClusterAck ack = (GetClusterAck) response.getPayload();
            String clientConfigs = ack.getClientConfigs();
            if (null != clientConfigs && !clientConfigs.isEmpty()) {
                this.clientConfigs = JSON.parseObject(clientConfigs, new
                        TypeReference<ConcurrentHashMap<String, DynamicClientConfig>>() {
                        });
            }

            byte identity = ack.getIdentity();
            boolean isChanged =  this.identity == 0 || this.identity != identity;
            if (isChanged) {
                this.identity = identity;

                byte ackDataCenter = ack.getDataCenter();
                if (ackDataCenter > 0 && dataCenter != ackDataCenter) {
                    dataCenter = ackDataCenter;
                }

                int ackMaxSize = ack.getMaxSize();
                if (ackMaxSize > 0 && ackMaxSize != maxSize) {
                    maxSize = ackMaxSize;
                }

                int ackInterval = ack.getInterval();
                if (ackInterval > 0 && ackInterval != updateClusterInterval) {
                    updateClusterInterval = ackInterval;
                    clusterUpdateTasks.setIdleTime(updateClusterInterval);
                }

                String topicConfigs = ack.getTopicConfigs();
                if (topicConfigs != null && !topicConfigs.equals(allTopicConfigStrings)) {
                    allTopicConfigStrings = topicConfigs;
                }
            }

            ConcurrentMap<String, TopicConfig> temConcurrentMap = JSON.parseObject(allTopicConfigStrings, new
                    TypeReference<ConcurrentHashMap<String, TopicConfig>>() {
            });

            if (logger.isDebugEnabled()) {
                logger.debug("update cluster from " + Ipv4.toAddress(socketAddress));
                for (BrokerCluster cluster : ack.getClusters()) {
                    logger.debug(String.format("update cluster topic %s [%s]", cluster.getTopic(), cluster.toString()));
                }
                logger.debug("update cluster topicConfig:" + ack.getTopicConfigs());
            }

            if (isChanged) {
                update(ack.getClusters());
                updateTopicConfig(temConcurrentMap);
            }

            if (!isStarted()) {
                return true;
            }
            save(file);
            onSuccess();
            return true;
        } catch (JMQException e) {
            // /请求超时/连接超时/连接出错/请求发送异常
            String solution = ExceptionHandler.getExceptionSolution(e.getMessage());
            logger.error(String.format("update cluster from %s error! message:%s", address, e.getMessage() + solution), e);
            return false;
        } catch (Exception e) {
            logger.warn("update cluster error!", e);
            return false;
        } finally {
            if (transport != null) {
                transport.stop();
            }
        }
    }

    /**
     * 成功更新集群
     */
    protected void onSuccess() {

    }

    protected void updateTopicConfig(ConcurrentMap<String, TopicConfig> curTopicConfigs) {
        writeLock.lock();
        try {
            if (!isStarted()) {
                return;
            }
            if (topicConfigs == null || curTopicConfigs == null) {
                return;
            }
            for (Map.Entry<String, TopicConfig> entry : curTopicConfigs.entrySet()) {
                TopicConfig oldTopicConfig = topicConfigs.get(entry.getKey());
                if (oldTopicConfig == null || oldTopicConfig.getQueues() != entry.getValue().getQueues()) {
                    eventManager.add(new ClusterEvent(EventType.TOPIC_QUEUE_CHANGE, entry.getKey(), entry.getValue().getQueues()));
                }
                topicConfigs.putIfAbsent(entry.getKey(), entry.getValue());
            }
            //这里暂时不做移除操作
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 更新监听器
     *
     * @param updates 集群
     */
    protected void update(List<BrokerCluster> updates) {
        writeLock.lock();
        try {
            if (!isStarted()) {
                return;
            }
            BrokerCluster oldCluster;
            BrokerGroup oldGroup;
            // 当前集群为空
            if (updates == null || updates.isEmpty()) {
                if (!clusters.isEmpty()) {
                    // 清除原有的集群信息
                    for (BrokerCluster brokerCluster : clusters.values()) {
                        for (BrokerGroup group : brokerCluster.getGroups()) {
                            eventManager.add(new ClusterEvent(EventType.REMOVE_BROKER, brokerCluster.getTopic(), group,
                                    brokerCluster.getQueues()));
                        }
                    }
                    clusters.clear();
                }
                return;
            }
            // 当前集群不为空
            List<String> sequentialAddresses = null;
            for (BrokerCluster cluster : updates) {
                TopicConfig topicConfig = getTopicConfig(cluster.getTopic());
                if (topicConfig != null && sequentialAddresses == null && topicConfig.checkSequential()
                        && cluster.getGroups() != null) {
                    sequentialAddresses = createSequentialAddress(cluster);
                }

                oldCluster = clusters.get(cluster.getTopic());
                if (oldCluster == null) {
                    // 新增
                    clusters.put(cluster.getTopic(), cluster);
                    for (BrokerGroup group : cluster.getGroups()) {
                        eventManager.add(new ClusterEvent(EventType.ADD_BROKER, cluster.getTopic(), group,
                                cluster.getQueues()));
                    }
                    // 最后广播队列数变更
                    eventManager.add(new ClusterEvent(EventType.QUEUE_CHANGE, cluster.getTopic(), cluster.getQueues()));
                } else {
                    // 修改
                    clusters.put(cluster.getTopic(), cluster);

                    // 判断删除的分组
                    for (BrokerGroup group : oldCluster.getGroups()) {
                        if (!cluster.contain(group)) {
                            eventManager.add(new ClusterEvent(EventType.REMOVE_BROKER, cluster.getTopic(), group));
                        }
                    }
                    // 遍历当前分组
                    for (BrokerGroup group : cluster.getGroups()) {
                        oldGroup = oldCluster.getGroup(group);
                        if (oldGroup == null) {
                            // 原来分组不存在
                            eventManager.add(new ClusterEvent(EventType.ADD_BROKER, cluster.getTopic(), group,
                                    cluster.getQueues()));
                        } else if (oldGroup.getPermission() != group.getPermission() || oldGroup.getWeight() != group
                                .getWeight()) {
                            // 原来分组存在，权限发生变更
                            eventManager.add(new ClusterEvent(EventType.UPDATE_BROKER, cluster.getTopic(), group,
                                    cluster.getQueues()));
                        }
                    }

                    // 最后广播队列数变更
                    if (oldCluster.getQueues() != cluster.getQueues()) {
                        eventManager
                                .add(new ClusterEvent(EventType.QUEUE_CHANGE, cluster.getTopic(), cluster.getQueues()));
                    }

                }
            }

            // 重置顺序服务地址
            this.sequentialAddress = sequentialAddresses;
        } finally {
            writeLock.unlock();
        }
    }

    private List<String> createSequentialAddress(final BrokerCluster cluster) {
        List<BrokerGroup> groups = cluster.getGroups();
        List<String> addresses = null;
        if (groups != null && groups.size() > 0) {

            addresses = new ArrayList<String>(groups.size());
            for (int i = 0; i < groups.size(); i++) {
                for (int j = groups.size()-1; j > i; j--) {
                    BrokerGroup low = groups.get(j);
                    BrokerGroup high = groups.get(j-1);
                    if (low.getPermission().ordinal() > high.getPermission().ordinal()) {
                        groups.set(j, high);
                        groups.set(j - 1, low);
                    }
                }
                BrokerGroup group = groups.get(i);
                String address = group.getMaster().getIp() + ":" + group.getMaster().getPort();
                addresses.add(address);
            }
        }

        return addresses;
    }

    /**
     * 保存到文件
     *
     * @param file 本地文件
     */
    protected void save(File file) {
        if (file == null) {
            return;
        }
        FileOutputStream output = null;
        ObjectOutputStream oos = null;
        try {
            output = new FileOutputStream(file);
            oos = new ObjectOutputStream(output);
            oos.writeObject(clusters);
            oos.writeObject(topicConfigs);
            oos.flush();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } finally {
            Close.close(oos, output);
        }
    }

    /**
     * 从文件加载
     *
     * @param file 本地文件
     */
    @SuppressWarnings("unchecked")
    protected void load(File file) {
        if (file == null || !file.exists()) {
            return;
        }
        FileInputStream input = null;
        ObjectInputStream ois = null;
        try {
            input = new FileInputStream(file);
            ois = new ObjectInputStream(input);
            this.clusters = (ConcurrentHashMap) ois.readObject();
            this.topicConfigs = (ConcurrentHashMap) ois.readObject();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage(), e);
        } finally {
            Close.close(ois, input);
        }
    }

    /**
     * 获取集群
     *
     * @param topic 主题
     */
    public BrokerCluster getCluster(String topic) {
        return clusters.get(topic);
    }

    /**
     * 获取主题配置
     *
     * @param topic 主题
     * @return 主题配置
     */
    public TopicConfig getTopicConfig(String topic) {
        TopicConfig topicConfig = null;
        if (topicConfigs != null && !topicConfigs.isEmpty()) {
            topicConfig = topicConfigs.get(topic);
        }
        return topicConfig;
    }

    /**
     * 增加监听器
     *
     * @param listener 监听器
     */
    public void addListener(EventListener<ClusterEvent> listener) {
        if (eventManager.addListener(listener)) {
            if (isStarted() && !clusters.isEmpty()) {
                for (BrokerCluster cluster : clusters.values()) {
                    for (BrokerGroup group : cluster.getGroups()) {
                        if (group.getPermission() != Permission.NONE) {
                            eventManager.add(new ClusterEvent(EventType.ADD_BROKER, cluster.getTopic(), group,
                                    cluster.getQueues()), listener);
                        }
                    }
                    // 最后广播队列数变更
                    eventManager.add(new ClusterEvent(EventType.QUEUE_CHANGE, cluster.getTopic(), cluster.getQueues()),
                            listener);
                }
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


    protected class UpdateClusterListener implements EventListener<ClusterEvent.EventType> {
        @Override
        public void onEvent(ClusterEvent.EventType event) {
            // 判断是否关闭
            if (!isStarted()) {
                return;
            }

            if (event.equals(EventType.GET_CLUSTER)) {
                updateCluster();
            }
        }
    }

    /**
     * 根据条件，立即或者异步更新集群信息
     *
     * @param now 是否立刻更新
     */
    public void updateCluster(boolean now) {
        if (now) {
            // 立即请求更新
            updateCluster();
        } else {
            // 添加到事件处理器异步更新
            clusterUpdateTasks.add(EventType.GET_CLUSTER);
        }
    }

    /**
     * 更新集群时间间隔
     *
     * @return 间隔时间
     */
    public int getUpdateClusterInterval() {
        return updateClusterInterval;
    }

    /**
     * get Dynamic Client Config by app
     * @param app app
     * @return DynamicClientConfig
     */
    private DynamicClientConfig getDynamicClientConfigByApp(final String app) {
        if (null == app || app.isEmpty() || clientConfigs.isEmpty()) {
            return null;
        }

        return clientConfigs.get(app);
    }

    /**
     * get DynamicProducerConfig by app and topic
     * @param localProducerConfig local ProducerConfig
     * @param app app name
     * @param topic topic name
     * @return ProducerConfig
     */
    public ProducerConfig getProducerConfig(ProducerConfig localProducerConfig, final String app, final String topic) {
        DynamicClientConfig dynamicClientConfig = getDynamicClientConfigByApp(app);
        if (null != dynamicClientConfig) {
            if (null == topic || topic.isEmpty()) {
                return localProducerConfig;
            }
            ProducerConfig remoteProducerConfig = dynamicClientConfig.getProducerConfigMap().get(topic);
            if (null != remoteProducerConfig) {
                int remoteRetryTimes = remoteProducerConfig.getRetryTimes();
                int remoteSendTimeout = remoteProducerConfig.getSendTimeout();
                Acknowledge remoteAcknowledge = remoteProducerConfig.getAcknowledge();
                if (remoteRetryTimes > 0) {
                    localProducerConfig.setRetryTimes(remoteRetryTimes);
                }
                if (remoteSendTimeout > 0) {
                    localProducerConfig.setSendTimeout(remoteSendTimeout);
                }
                if (null != remoteAcknowledge) {
                    localProducerConfig.setAcknowledge(remoteAcknowledge);
                }
            }
        }

        return localProducerConfig;
    }

    /**
     * get DynamicConsumerConfig by app and topic
     * @param localConsumerConfig local ConsumerConfig
     * @param app app name
     * @param topic topic name
     * @return ConsumerConfig
     */
    public ConsumerConfig getConsumerConfig(ConsumerConfig localConsumerConfig, final String app, final String topic) {
        DynamicClientConfig dynamicClientConfig = getDynamicClientConfigByApp(app);
        if (null != dynamicClientConfig) {
            if (null == topic || topic.isEmpty()) {
                return null;
            }
            ConsumerConfig remoteConsumerConfig = dynamicClientConfig.getConsumerConfigMap().get(topic);

            if (null != remoteConsumerConfig) {
                int remoteLongPull = remoteConsumerConfig.getLongPull();
                int remotePullEmptySleep = remoteConsumerConfig.getPullEmptySleep();
                int remotePullTimeout = remoteConsumerConfig.getPullTimeout();
                int remoteMaxConcurrent = remoteConsumerConfig.getMaxConcurrent();
                int remoteMinConcurrent = remoteConsumerConfig.getMinConcurrent();
                RetryPolicy remoteRetryPolicy = remoteConsumerConfig.getRetryPolicy();
                if (remoteLongPull > 0) {
                    localConsumerConfig.setLongPull(remoteLongPull);
                }
                if (remotePullEmptySleep > 0) {
                    localConsumerConfig.setPullEmptySleep(remotePullEmptySleep);
                }
                if (remotePullTimeout > 0) {
                    localConsumerConfig.setPullTimeout(remotePullTimeout);
                }
                if (remoteMaxConcurrent > 0) {
                    localConsumerConfig.setMaxConcurrent(remoteMaxConcurrent);
                }
                if (remoteMinConcurrent > 0) {
                    localConsumerConfig.setMinConcurrent(remoteMinConcurrent);
                }
                if (null != remoteRetryPolicy) {
                    localConsumerConfig.setRetryPolicy(remoteRetryPolicy);
                }
            }
        }

        return localConsumerConfig;
    }

}