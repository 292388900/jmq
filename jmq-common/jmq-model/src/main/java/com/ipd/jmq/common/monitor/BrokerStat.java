package com.ipd.jmq.common.monitor;

import com.ipd.jmq.common.cluster.ClusterRole;
import com.ipd.jmq.common.cluster.Permission;
import com.ipd.jmq.common.cluster.RetryType;
import com.ipd.jmq.common.cluster.SyncMode;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * broker统计信息
 *
 * @author xuzhenhua
 */
public class BrokerStat implements Serializable {

    private static final long serialVersionUID = -4637157513730009816L;
    // broker名称
    private String name;
    // broker总连接数
    private AtomicLong connection = new AtomicLong(0);
    // 服务端入队消息数量
    private AtomicLong enQueue = new AtomicLong(0);
    // 服务端入队消息大小(单位字节)
    private AtomicLong enQueueSize = new AtomicLong(0);
    // 服务端出队消息数量
    private AtomicLong deQueue = new AtomicLong(0);
    // 服务端出队消息大小(单位字节)
    private AtomicLong deQueueSize = new AtomicLong(0);
    // 积压数量
    private AtomicLong pending = new AtomicLong(0);
    // jvm堆内存总数 ,单位MB
    private AtomicLong jvmHeapTotal = new AtomicLong(0);
    // jvm已用堆内存,单位MB
    private AtomicLong jvmHeapUsed = new AtomicLong(0);
    // 分配给jmq的总磁盘大小(单位字节)
    private AtomicLong diskTotal = new AtomicLong(0);
    // 分配给jmq的已用磁盘大小(单位字节)
    private AtomicLong diskUsed = new AtomicLong(0);
    // mmap内存总数(单位字节)
    private AtomicLong mmapTotal = new AtomicLong(0);
    // mmap已用内存(单位字节)
    private AtomicLong mmapUsed = new AtomicLong(0);
    // 磁盘写入目前的速度(单位字节)
    private double diskWriteSpeed ;
    // 复制目前的速度(单位字节)
    private double replicationSpeed ;
    //最大日志偏移位置
    private long maxJournalOffset;
    //最小日志偏移位置
    private long minJournalOffset;

    private boolean hasBadBlock;

    private int port;
    // 分组
    private String group;
    // 别名
    private String alias;
    // 数据中心
    private byte dataCenter;
    // 集群角色
    private ClusterRole role;
    // 权限
    private Permission permission;
    // 重试类型
    private RetryType retryType;
    // 同步方式
    private SyncMode syncMode;


    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public byte getDataCenter() {
        return dataCenter;
    }

    public void setDataCenter(byte dataCenter) {
        this.dataCenter = dataCenter;
    }

    public ClusterRole getRole() {
        return role;
    }

    public void setRole(ClusterRole role) {
        this.role = role;
    }

    public Permission getPermission() {
        return permission;
    }

    public void setPermission(Permission permission) {
        this.permission = permission;
    }

    public RetryType getRetryType() {
        return retryType;
    }

    public void setRetryType(RetryType retryType) {
        this.retryType = retryType;
    }

    public SyncMode getSyncMode() {
        return syncMode;
    }

    public void setSyncMode(SyncMode syncMode) {
        this.syncMode = syncMode;
    }

    // topic统计信息列表
    private ConcurrentMap<String, TopicStat> topicStats = new ConcurrentHashMap<String, TopicStat>();

    public BrokerStat() {
    }

    public BrokerStat(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public AtomicLong getConnection() {
        return connection;
    }

    public void setConnection(AtomicLong connection) {
        this.connection = connection;
    }

    public AtomicLong getEnQueue() {
        return enQueue;
    }

    public void setEnQueue(AtomicLong enQueue) {
        this.enQueue = enQueue;
    }

    public AtomicLong getEnQueueSize() {
        return enQueueSize;
    }

    public void setEnQueueSize(AtomicLong enQueueSize) {
        this.enQueueSize = enQueueSize;
    }

    public AtomicLong getDeQueue() {
        return deQueue;
    }

    public void setDeQueue(AtomicLong deQueue) {
        this.deQueue = deQueue;
    }

    public AtomicLong getDeQueueSize() {
        return deQueueSize;
    }

    public void setDeQueueSize(AtomicLong deQueueSize) {
        this.deQueueSize = deQueueSize;
    }

    public AtomicLong getPending() {
        return pending;
    }

    public void setPending(AtomicLong pending) {
        this.pending = pending;
    }

    public AtomicLong getJvmHeapTotal() {
        return jvmHeapTotal;
    }

    public void setJvmHeapTotal(AtomicLong jvmHeapTotal) {
        this.jvmHeapTotal = jvmHeapTotal;
    }

    public AtomicLong getJvmHeapUsed() {
        return jvmHeapUsed;
    }

    public void setJvmHeapUsed(AtomicLong jvmHeapUsed) {
        this.jvmHeapUsed = jvmHeapUsed;
    }

    public AtomicLong getDiskTotal() {
        return diskTotal;
    }

    public void setDiskTotal(AtomicLong diskTotal) {
        this.diskTotal = diskTotal;
    }

    public AtomicLong getDiskUsed() {
        return diskUsed;
    }

    public void setDiskUsed(AtomicLong diskUsed) {
        this.diskUsed = diskUsed;
    }

    public AtomicLong getMmapTotal() {
        return mmapTotal;
    }

    public void setMmapTotal(AtomicLong mmapTotal) {
        this.mmapTotal = mmapTotal;
    }

    public AtomicLong getMmapUsed() {
        return mmapUsed;
    }

    public void setMmapUsed(AtomicLong mmapUsed) {
        this.mmapUsed = mmapUsed;
    }

    public double getDiskWriteSpeed() {
        return diskWriteSpeed;
    }

    public void setDiskWriteSpeed(double diskWriteSpeed) {
        this.diskWriteSpeed = diskWriteSpeed;
    }

    public double getReplicationSpeed() {
        return replicationSpeed;
    }

    public void setReplicationSpeed(double replicationSpeed) {
        this.replicationSpeed = replicationSpeed;
    }

    public ConcurrentMap<String, TopicStat> getTopicStats() {
        return topicStats;
    }

    public void setTopicStats(ConcurrentMap<String, TopicStat> topicStats) {
        this.topicStats = topicStats;
    }

    public TopicStat getAndCreateTopicStat(String topic) {
        if (topic == null || topic.isEmpty()) {
            return null;
        }
        TopicStat topicStat = topicStats.get(topic);
        if (topicStat == null) {
            topicStat = new TopicStat(topic);
            TopicStat old = topicStats.putIfAbsent(topic, topicStat);
            if (old != null) {
                topicStat = old;
            }
        }
        return topicStat;
    }

    public long getMaxJournalOffset() {
        return maxJournalOffset;
    }

    public void setMaxJournalOffset(long maxJournalOffset) {
        this.maxJournalOffset = maxJournalOffset;
    }

    public long getMinJournalOffset() {
        return minJournalOffset;
    }

    public void setMinJournalOffset(long minJournalOffset) {
        this.minJournalOffset = minJournalOffset;
    }

    public boolean isHasBadBlock() {
        return hasBadBlock;
    }

    public void setHasBadBlock(boolean hasBadBlock) {
        this.hasBadBlock = hasBadBlock;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BrokerStat{");
        sb.append("name='").append(name).append('\'');
        sb.append(", connection=").append(connection);
        sb.append(", enQueue=").append(enQueue);
        sb.append(", enQueueSize=").append(enQueueSize);
        sb.append(", deQueue=").append(deQueue);
        sb.append(", deQueueSize=").append(deQueueSize);
        sb.append(", pending=").append(pending);
        sb.append(", jvmHeapTotal=").append(jvmHeapTotal);
        sb.append(", jvmHeapUsed=").append(jvmHeapUsed);
        sb.append(", diskTotal=").append(diskTotal);
        sb.append(", diskUsed=").append(diskUsed);
        sb.append(", mmapTotal=").append(mmapTotal);
        sb.append(", mmapUsed=").append(mmapUsed);
        sb.append(", diskWriteSpeed=").append(diskWriteSpeed);
        sb.append(", replicationSpeed=").append(replicationSpeed);
        sb.append(", topicStats=").append(topicStats);
        sb.append(", maxJournalOffset=").append(maxJournalOffset);
        sb.append(", minJournalOffset=").append(minJournalOffset);
        sb.append(", hasBadBlock=").append(hasBadBlock);
        sb.append('}');
        return sb.toString();
    }
}
