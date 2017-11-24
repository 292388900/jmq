package com.ipd.jmq.server.broker.monitor.impl;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.cluster.ClusterRole;
import com.ipd.jmq.common.lb.ClientSession;
import com.ipd.jmq.common.lb.ClientType;
import com.ipd.jmq.common.lb.TopicSession;
import com.ipd.jmq.common.network.v3.session.Consumer;
import com.ipd.jmq.common.network.v3.session.Producer;
import com.ipd.jmq.server.broker.SessionManager;
import com.ipd.jmq.server.broker.monitor.api.SessionMonitor;
import com.ipd.jmq.common.monitor.AppStat;
import com.ipd.jmq.common.monitor.BrokerStat;
import com.ipd.jmq.common.monitor.Client;
import com.ipd.jmq.common.monitor.TopicStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by zhangkepeng on 16-11-30.
 */
public class SessionMonitorImpl implements SessionMonitor{

    private static final Logger logger = LoggerFactory.getLogger(SessionMonitorImpl.class);

    // 统计连接数
    protected Map<String, Client> connections;
    // 会话管理
    protected SessionManager sessionManager;
    // 本地分片
    protected Broker broker;
    // 分片统计
    protected BrokerStat brokerStat;

    public SessionMonitorImpl(Builder builder) {
        this.sessionManager = builder.sessionManager;
        this.connections = builder.connections;
        this.broker = builder.broker;
        this.brokerStat = builder.brokerStat;
    }

    @Override
    public List<TopicSession> getTopicSessionsOnBroker() {
        String brokerGroup = broker.getGroup();
        String brokerIp = broker.getIp();
        boolean isMaster = broker.getRole() == ClusterRole.MASTER;

        List<TopicSession> topicSessions = new ArrayList<TopicSession>();
        Set<String> topics = new HashSet<String>();
        List<Consumer> consumers = sessionManager.getConsumer();
        List<Producer> producers = sessionManager.getProducer();

        for (Consumer c : consumers) {
            topics.add(c.getJoint().getTopic());
        }

        for (String topic : topics) {
            Map<String, Map<ClientType, List<ClientSession>>> appSessions =
                    new HashMap<String, Map<ClientType, List<ClientSession>>>();

            TopicSession topicSession = new TopicSession();
            topicSession.setTopic(topic);
            topicSession.setGroup(brokerGroup);
            topicSession.setBrokerIP(brokerIp);
            topicSession.setMaster(isMaster);
            topicSession.setTime(System.currentTimeMillis());
            topicSession.setAppSessions(appSessions);

            for (Consumer consumer : consumers) {
                String connectionId = consumer.getConnectionId();
                String app = consumer.getJoint().getApp();
                topicSession.addSession(ClientType.CONSUMER, app, connectionId);
            }
            for (Producer producer : producers) {
                String connectionId = producer.getConnectionId();
                String app = producer.getApp();
                topicSession.addSession(ClientType.PRODUCER, app, connectionId);
            }

            topicSessions.add(topicSession);

        }

        return topicSessions;
    }

    @Override
    public List<Client> getConnections() {
        List<Client> clients = new ArrayList<Client>();
        for (Map.Entry<String, Client> entry : connections.entrySet()) {
            if (sessionManager.getConnectionById(entry.getKey()) != null) {
                clients.add(entry.getValue());
            }
        }
        return clients;
    }

    @Override
    public List<Client> getConnections(final String topic, final String app) {
        List<Client> clients = new ArrayList<Client>();
        if (topic == null || topic.isEmpty() || app == null || app.isEmpty()) {
            return clients;
        }
        TopicStat topicStat = brokerStat.getTopicStats().get(topic);
        if (topicStat != null) {
            AppStat appStat = topicStat.getAppStats().get(app);
            if (appStat != null) {
                clients.addAll(appStat.getClients().values());
            }
        }
        List<Client> retClients = new ArrayList<Client>();
        for (Client c : clients) {
            if (sessionManager.getConnectionById(c.getConnectionId()) != null) {
                retClients.add(c);
            }
        }
        return retClients;
    }

    @Override
    public void closeProducer(final String topic, final String app) {
        sessionManager.closeProducer(topic, app);
    }

    @Override
    public void closeConsumer(final String topic, final String app) {
        sessionManager.closeConsumer(topic, app);
    }

    public static class Builder {

        // 统计连接数
        protected Map<String, Client> connections;
        // 会话管理
        protected SessionManager sessionManager;
        // 本地分片
        protected Broker broker;
        // 分片统计
        protected BrokerStat brokerStat;

        public Builder(Map<String, Client> connections, SessionManager sessionManager) {
            this.connections = connections;
            this.sessionManager = sessionManager;
        }

        public Builder broker(Broker broker) {
            this.broker = broker;
            return this;
        }

        public Builder brokerStat(BrokerStat brokerStat) {
            this.brokerStat = brokerStat;
            return this;
        }

        public SessionMonitor build() {
            return new SessionMonitorImpl(this);
        }
    }
}
