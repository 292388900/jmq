package com.ipd.jmq.server.broker.election;


import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.cluster.BrokerGroup;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.toolkit.URL;
import com.ipd.jmq.toolkit.concurrent.EventBus;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 固定主从
 *
 * @author tianya
 * @since 2014-04-21
 */
public class FixedRoleDecider extends Service implements RoleDecider {
    private static final Logger logger = LoggerFactory.getLogger(FixedRoleDecider.class);
    // 集群管理器
    protected ClusterManager clusterManager;
    // 配置数据
    protected BrokerConfig brokerConfig;
    // 当前集群状态(并发)
    protected RoleEvent roleEvent;
    // 事件管理器
    protected EventBus<RoleEvent> eventManager = new EventBus<RoleEvent>("RoleDecider");
    // URL参数
    protected URL url;

    @Override
    protected void validate() throws Exception {
        super.validate();
        if (clusterManager == null) {
            throw new IllegalStateException("clusterManager can not be null");
        }
        BrokerGroup brokerGroup = clusterManager.getBrokerGroup();
        List<Broker> replicas = brokerGroup.getSlaves();
        replicas.addAll(brokerGroup.getBackups());
        this.roleEvent = new RoleEvent(brokerGroup.getMaster(), replicas, null);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        eventManager.start();
        logger.info("fixed role decider is started");
    }

    @Override
    protected void doStop() {
        super.doStop();
        eventManager.stop();
        logger.info("fixed role decider is stopped");
    }

    @Override
    public String getType() {
        return "fix";
    }

    @Override
    public void setUrl(URL url) {
        this.url = url;
    }

    @Override
    public void setClusterManager(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    @Override
    public void setBrokerConfig(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
    }

    @Override
    public void addListener(EventListener<RoleEvent> listener) {
        if (eventManager.addListener(listener)) {
            if (roleEvent != null) {
                eventManager.add(roleEvent, listener);
            }
        }
    }

    @Override
    public void removeListener(EventListener<RoleEvent> listener) {
        eventManager.removeListener(listener);
    }


    @Override
    public void releaseMaster(Broker lastMaster) {

    }
}