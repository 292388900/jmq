package com.ipd.jmq.server.broker.retry;

import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.context.ContextManager;
import com.ipd.jmq.registry.listener.LeaderListener;
import com.ipd.jmq.toolkit.concurrent.Scheduler;
import com.ipd.jmq.toolkit.service.Service;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created by guoliang5 on 2017/1/16.
 */
public class RetryManagerDummy extends Service implements RetryManager {
    public RetryManagerDummy(ClusterManager clusterManager, ContextManager contextManager, Scheduler scheduler, BrokerConfig config) {
        super();
    }

    @Override
    public void addRetry(String topic, String app, BrokerMessage[] messages, String exception) throws JMQException {
        throw new JMQException(JMQCode.CN_PLUGIN_NOT_IMPLEMENT);
    }

    @Override
    public void retrySuccess(String topic, String app, long[] messageIds) throws JMQException {
        throw new JMQException(JMQCode.CN_PLUGIN_NOT_IMPLEMENT);
    }

    @Override
    public void retryError(String topic, String app, long[] messageIds) throws JMQException {
        throw new JMQException(JMQCode.CN_PLUGIN_NOT_IMPLEMENT);
    }

    @Override
    public void retryExpire(String topic, String app, long[] messageIds) throws JMQException {
        throw new JMQException(JMQCode.CN_PLUGIN_NOT_IMPLEMENT);
    }

    @Override
    public List<BrokerMessage> getRetry(String topic, String app, short count, long startId) throws JMQException {
        return null;
    }

    @Override
    public int getRetry(String topic, String app) throws JMQException {
        return 0;
    }

    @Override
    public void clearCache(String topic, String app) {

    }

    @Override
    public BlockingQueue<BrokerMessage> viewCachedRetry(String topic, String app) {
        return null;
    }

    @Override
    public void reloadRetryCache(String topic, String app) {

    }

    @Override
    public void setLeaderListener(LeaderListener leaderListener) {

    }
}
