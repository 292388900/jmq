package com.ipd.jmq.client.connection;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.cluster.BrokerGroup;
import com.ipd.jmq.common.cluster.Permission;
import com.ipd.jmq.common.network.RoundRobinFailoverPolicy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 分组故障切换策略
 */
public class GroupFailoverPolicy extends RoundRobinFailoverPolicy {
    // 集群分组
    protected BrokerGroup group;
    // 权限
    protected Permission permission;

    public GroupFailoverPolicy(BrokerGroup group, Permission permission) {
        if (group == null) {
            throw new IllegalArgumentException("group can not be null");
        }
        if (permission == null) {
            throw new IllegalArgumentException("permission can not be null");
        }
        this.addresses = new ArrayList<String>();
        this.group = group;
        this.permission = permission;

        List<Broker> brokers = getCandidates();
        for (Broker broker : brokers) {
            addresses.add(broker.getName());
        }
    }

    /**
     * 获取候选者
     *
     * @return 候选者列表
     */
    protected List<Broker> getCandidates() {
        // 按读写权限降序排序
        List<Broker> brokers = new ArrayList<Broker>();
        if (permission == Permission.WRITE) {
            // 写权限，考虑主从切换
            for (Broker broker : group.getBrokers()) {
                if (broker.getRole().isCandidate()) {
                    brokers.add(broker);
                }
            }
            // 按权限优先级降序排序(FULL,WRITE,READ,NONE)
            Collections.sort(brokers, new Comparator<Broker>() {
                @Override
                public int compare(Broker o1, Broker o2) {
                    return -(o1.getPermission().ordinal() - o2.getPermission().ordinal());
                }
            });
        } else if (permission == Permission.READ) {
            brokers.addAll(group.getBrokers());
            // 按权限优先级降序排序(FULL,READ,WRITE,NONE)
            Collections.sort(brokers, new Comparator<Broker>() {
                @Override
                public int compare(Broker o1, Broker o2) {
                    if (o1.getPermission() == Permission.FULL) {
                        if (o2.getPermission() == Permission.FULL) {
                            return 0;
                        }
                        return -1;
                    } else if (o1.getPermission() == Permission.READ) {
                        if (o2.getPermission() == Permission.FULL) {
                            return 1;
                        } else if (o2.getPermission() == Permission.READ) {
                            return o1.getRole().ordinal() - o2.getRole().ordinal();
                        }
                        return -1;
                    } else if (o1.getPermission() == Permission.WRITE) {
                        if (o2.getPermission() == Permission.FULL) {
                            return 1;
                        } else if (o2.getPermission() == Permission.READ) {
                            return 1;
                        } else if (o2.getPermission() == Permission.WRITE) {
                            return o1.getRole().ordinal() - o2.getRole().ordinal();
                        }
                        return -1;
                    } else if (o1.getPermission() == Permission.NONE) {
                        if (o2.getPermission() == Permission.FULL) {
                            return 1;
                        } else if (o2.getPermission() == Permission.READ) {
                            return 1;
                        } else if (o2.getPermission() == Permission.WRITE) {
                            return 1;
                        }
                        return o1.getRole().ordinal() - o2.getRole().ordinal();
                    }
                    return 1;
                }
            });
        }
        return brokers;
    }
}
