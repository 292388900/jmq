package com.ipd.jmq.server.broker.monitor.api;

import com.ipd.jmq.common.cluster.ClusterRole;
import com.ipd.jmq.common.monitor.BrokerExe;
import com.ipd.jmq.common.monitor.BrokerPerf;
import com.ipd.jmq.common.monitor.BrokerStat;
import com.ipd.jmq.common.monitor.ReplicationState;

import java.util.List;

/**
 * Created by zhangkepeng on 16-11-30.
 */
public interface PartitionMonitor {

    /**
     * 获得broker的统计信息
     *
     * @return Broker统计信息
     */
    BrokerStat getBrokerStat();

    /**
     * 获得broker的性能统计信息
     *
     * @return 性能统计
     */
    BrokerPerf getPerformance();

    /**
     * 获得broker异常统计信息
     *
     * @return 异常统计
     */
    BrokerExe getBrokerExe();

    /**
     * 获得主从复制级别
     *
     * @return 复制级别
     */
    List<ReplicationState> getReplicationStates();

    /**
     * 获取复制分片数
     *
     * @return
     */
    boolean hasReplicas();

    /**
     * 获取集群角色
     *
     * @return 集群角色
     */
    ClusterRole getBrokerRole();

    /**
     * 获取Broker版本
     * @return String 版本号
     */
    String getBrokerVersion();

    /**
     * 重启次数和启动时间
     * @return
     */
    String restartAndStartTime();

    /**
     * 返回StoreConfig的JSON串
     * @return
     */
    String getStoreConfig();
    /**
     *
     */
    void setBrokerStat(BrokerStat brokerStat);
}
