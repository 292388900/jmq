package com.ipd.jmq.common.network.kafka.utils;

import com.alibaba.fastjson.JSON;
import com.ipd.jmq.common.network.kafka.model.KafkaLiveBroker;
import com.ipd.jmq.registry.PathData;
import com.ipd.jmq.registry.Registry;
import com.ipd.jmq.registry.RegistryException;
import com.ipd.jmq.registry.listener.ChildrenEvent;
import com.ipd.jmq.registry.listener.ChildrenListener;
import com.ipd.jmq.toolkit.lang.Charsets;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by zhangkepeng on 16-8-2.
 */
public class ZKUtils {
    private static final Logger logger = LoggerFactory.getLogger(ZKUtils.class);

    // 注册中心brokers ids路径
    public static String brokerIdsPath = "/jmq/brokers/ids";
    // 注册中心brokers topics路径
    public static String brokerTopicsPath = "/jmq/brokers/topics";
    // 注册中心brokers role路径
    public static String brokerRoleChangePath = "/jmq/brokers/role";
    // 注册中心brokers leader路径
    public static String brokerLeaderPath = "/jmq/brokers/leader";
    // 注册中心controller_epoll
    public static String controllerEpochPath = "/jmq/controller_epoch";
    // 注册中心controller
    public static String controllerPath = "/jmq/controller";
    // 注册中心consumer offset路径
    public static String consumersPath = "/jmq/consumers";
    // 注册中心topic partition路径
    public static String topicPartitionPath = "/jmq/queues_zip";

    public static String getConsumerGroupPath(String group) {
        return consumersPath + "/" + group;
    }

    public static String getGroupTopicPath(String group, String topic) {
        return getConsumerGroupPath(group) + "/offsets/" + topic;
    }

    public static String getBrokerIdsPath(int id) {
        return brokerIdsPath + "/" + id;
    }

    public static String getTopicPath(String topic) {
        return brokerTopicsPath + "/" + topic;
    }

    public static String getTopicPartitionsPath(String topic) {
        return getTopicPath(topic) + "/partitions";
    }

    public static String getTopicPartitionPath(String topic, int partitionId) {
        return getTopicPartitionsPath(topic) + "/" + partitionId;
    }

    public static String getTopicPartitionLeaderAndIsrPath(String topic, int partitionId) {
        return getTopicPartitionPath(topic, partitionId) + "/" + "state";
    }

    public static String getRoleChangeParentPath() {
        return brokerRoleChangePath + "/" + "master";
    }

    public static String getRoleChangePath() {
        return getRoleChangeParentPath() + "/" + "id";
    }

    public static String getBrokerLeaderPath() {
        return brokerLeaderPath;
    }

    public static String getControllerEpochPath() {
        return controllerEpochPath;
    }

    public static String getControllerPath() {
        return controllerPath;
    }


    public static String getTopicPartitionPath() {
        return topicPartitionPath;
    }

    public static void setTopicPartitionPath(String topicPartitionPath) {
        ZKUtils.topicPartitionPath = topicPartitionPath;
    }

    /**
     * 节点是否存在
     */
    public static boolean isExists(Registry registry, String path) {
        try {
            return registry.exists(path);
        } catch (RegistryException e) {
            return false;
        }
    }

    /**
     * 判断当前节点是否是leader
     * @return
     */
    public static boolean isLeader(Registry registry) {
        try {
            return registry.isLeader(getBrokerLeaderPath());
        } catch (RegistryException e) {
            return false;
        }
    }

    /**
     * 读取数据
     *
     * @param registry
     * @param path
     * @return
     */
    public static String getDataPersistentPath(Registry registry, String path) {
        try {
            PathData pathData = registry.getData(path);
            if (pathData != null) {
                String content = new String(pathData.getData(), Charsets.UTF_8);
                return content;
            }
        } catch (RegistryException e) {
            logger.error(String.format("read persisstent path %s error", path), e);
        }
        return null;
    }

    /**
     * 获取节点数据
     * @param registry
     * @param path
     * @return
     */
    public static byte[] getDataFromRegistry(Registry registry, String path) {
        try {
            PathData pathData = registry.getData(path);
            if (pathData != null) {
                byte[] bytes = pathData.getData();
                return bytes;
            }
        } catch (RegistryException e) {
            logger.error(String.format("read data path %d exception", path), e);
        }
        return null;
    }

    /**
     * 获取子节点数据
     * @param registry
     * @param path
     * @return
     */
    public static List<byte[]> getChildDataFromRegistry(Registry registry, String path) {
        try {
            List<PathData> pathDatas = registry.getChildData(path);
            if (pathDatas != null && !pathDatas.isEmpty()) {
                List<byte[]> childBytes = new ArrayList<>();
                for (PathData pathData : pathDatas) {
                    if (pathData.getData() != null && pathData.getData().length != 0) {
                        childBytes.add(pathData.getData());
                    }
                }
                return childBytes;
            }
        } catch (RegistryException e) {
            logger.error("get child data from registry exception");
        }
        return null;
    }

    /**
     * 创建Persistent节点
     *
     * @param registry
     * @param path
     * @param data
     */
    public static boolean createPersistentPath(Registry registry, String path, String data) {
        try {
            registry.create(path, data.getBytes(Charsets.UTF_8));
            return true;
        } catch (RegistryException e) {
            logger.error(String.format("create persistent path %s error", path), e);
            return false;
        }
    }

    /**
     * 更新节点数据
     *
     * @param registry
     * @param path
     * @param data
     */
    public static boolean updatePersistentPath(Registry registry, String path, String data) {
        try {
            registry.update(path, data.getBytes(Charsets.UTF_8));
            return true;
        } catch (RegistryException e) {
            logger.error(String.format("update persisstent path %s error", path), e);
            return false;
        }
    }

    /**
     * 更新节点数据
     *
     * @param registry
     * @param path
     * @param childData 子节点数据
     * @param parentData 父节点数据
     */
    public static boolean updatePersistentPath(Registry registry, String path, String childData, String parentData) {
        try {
            registry.update(path, childData.getBytes(Charsets.UTF_8), parentData.getBytes(Charsets.UTF_8));
            return true;
        } catch (RegistryException e) {
            logger.error(String.format("update persisstent path %s error", path), e);
            return false;
        }
    }

    /**
     * 删除节点
     *
     * @param registry
     * @param path
     */
    public static boolean deletePersistentPath(Registry registry, String path) {
        try {
            registry.delete(path);
            return true;
        } catch (RegistryException e) {
            logger.error(String.format("delete persisstent path %s error", path), e);
            return false;
        }
    }

    /**
     * 获取指定节点下孩子节点的path
     *
     * @param path 全路径
     * @return 孩子节点的path，不包括父节点
     * @throws RegistryException
     */
    public static List<String> getChildren(Registry registry, String path) {
        try {
            return registry.getChildren(path);
        } catch (RegistryException e) {
            logger.error(String.format("get children path %s error", path), e);
            return null;
        }
    }

    /**
     * 注册存活节点
     * @param registry
     * @param id
     * @param host
     * @param port
     * @param jmxPort
     * @throws InterruptedException
     */
    // 注册存活
    public static void registerBrokerInZk(Registry registry, int id, String host, int port, int jmxPort) throws InterruptedException {
        String path = getBrokerIdsPath(id);
        String timestamp = String.valueOf(SystemClock.now());
        KafkaLiveBroker kafkaLiveBroker = new KafkaLiveBroker();
        kafkaLiveBroker.setVersion(1);
        kafkaLiveBroker.setHost(host);
        kafkaLiveBroker.setPort(port);
        kafkaLiveBroker.setJmx_port(jmxPort);
        kafkaLiveBroker.setTimestamp(timestamp);
        String brokerInfo = JSON.toJSONString(kafkaLiveBroker);
        registry.createLive(path, brokerInfo.getBytes(Charsets.UTF_8));
        // 确保创建成功
        CountDownLatch latch = new CountDownLatch(1);
        ChildListener listener = new ChildListener(path, latch, ChildrenEvent.ChildrenEventType.CHILD_CREATED);
        registry.addListener(brokerIdsPath, listener);
        latch.await();
        registry.removeListener(brokerIdsPath, listener);
    }

    // 注销存活
    public static void deregisterBrokerInZk(Registry registry, int id) throws InterruptedException{
        String path = getBrokerIdsPath(id);
        CountDownLatch latch = new CountDownLatch(1);
        ChildListener listener = new ChildListener(path, latch, ChildrenEvent.ChildrenEventType.CHILD_REMOVED);
        registry.addListener(brokerIdsPath, listener);
        registry.deleteLive(path);
        latch.await();
        registry.removeListener(brokerIdsPath, listener);
        logger.info(String.format("Deregistered broker %d at path %s.", id, path));
    }

    private static class ChildListener implements ChildrenListener {
        private final String path;
        private final CountDownLatch latch;
        private final ChildrenEvent.ChildrenEventType eventType;

        public ChildListener(String path, CountDownLatch latch, ChildrenEvent.ChildrenEventType eventType) {
            this.path = path;
            this.latch = latch;
            this.eventType = eventType;
        }

        @Override
        public void onEvent(ChildrenEvent event) {
            if (event.getType() == eventType) {
                if (event.getPath().equals(path)) {
                    latch.countDown();
                }
            }
        }
    }
}
