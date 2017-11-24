package com.ipd.jmq.server.broker.election;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.cluster.ClusterRole;

import java.util.ArrayList;
import java.util.List;

/**
 * 集群状态
 *
 * @author tianya
 * @since 2014-04-21
 */
public class RoleEvent {
    // 当前主节点
    private Broker master;
    // 当前从节点
    private List<Broker> slaves;
    // 记录上一次主节点
    private Broker lastMaster;
    // 是否需要从Agent同步数据
    private boolean syncFromAgent;

    public RoleEvent() {

    }

    public RoleEvent(Broker master, List<Broker> slaves, Broker lastMaster) {
        this.master = master;
        this.slaves = slaves;
        this.lastMaster = lastMaster;
    }

    public Broker getMaster() {
        return this.master;
    }

    public void setMaster(Broker master) {
        this.master = master;
    }

    public List<Broker> getSlaves() {
        return this.slaves;
    }

    public void setSlaves(List<Broker> slaves) {
        this.slaves = slaves;
    }

    public Broker getLastMaster() {
        return this.lastMaster;
    }

    public void setLastMaster(Broker lastMaster) {
        this.lastMaster = lastMaster;
    }

    public boolean isSyncFromAgent() {
        return this.syncFromAgent;
    }

    public void setSyncFromAgent(boolean syncFromAgent) {
        this.syncFromAgent = syncFromAgent;
    }

    public void addSlave(Broker broker) {
        if (broker == null) {
            return;
        }
        if (slaves == null) {
            slaves = new ArrayList<Broker>();
        }
        slaves.add(broker);
    }

    public ClusterRole getRole(Broker broker) {
        if (broker == null || master == null) {
            return ClusterRole.NONE;
        }
        String name = broker.getName();
        if (name.equals(master.getName())) {
            return ClusterRole.MASTER;
        }
        for (Broker slave : slaves) {
            if (slave.getName().equals(name)) {
                if (slave.getRole() == ClusterRole.BACKUP) {
                    return ClusterRole.BACKUP;
                } else {
                    return ClusterRole.SLAVE;
                }
            }
        }
        return ClusterRole.NONE;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        if (master!=null) {
        	sb.append(master.getIp()).append(':').append(master.getPort());
		}
        if(slaves == null)return sb.toString();
        for (Broker broker : slaves) {
            if(broker!=null){
                sb.append(',').append(broker.getIp()).append(':').append(broker.getPort());
            }
        }
        return sb.toString();
    }
}