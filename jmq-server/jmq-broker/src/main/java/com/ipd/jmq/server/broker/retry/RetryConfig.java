package com.ipd.jmq.server.broker.retry;


import com.ipd.jmq.server.broker.context.BrokerContext;
import com.ipd.jmq.server.context.ContextEvent;
import com.ipd.jmq.toolkit.concurrent.EventListener;

import java.util.HashSet;
import java.util.Set;

/**
 * 重试服务配置
 */
public class RetryConfig implements EventListener<ContextEvent> {
    // 消费者重试选举节点
    protected String retryLeaderPath = "/jmq/retry_consumer";
    // 重试管理器监听器
    protected RetryManagerListener retryManagerListener;
    // 重试数据队列大小
    protected int retryQueueSize = 100;
    // 获取重试批量大小
    protected int retryFetchSize = 20;
    // 获取重试数据的线程数
    protected int retryFetchThreads = 5;
    // 获取重试数据的时间间隔
    protected int retryFetchInterval = 5000;
    // 清理缓存重试数据的时间
    protected int retryClearInterval = 1000 * 60 * 5;
    // 重试的表数量
    protected int retryTables = 20;
    // 远程获取重试数据的超时时间
    protected int retryTimeout = 1000 * 10;
    //统计的间隔时间
    protected long statTimeWindowSize = 1000*10;
    //间隔时间内的限流次数
    protected int limitTimes = 5;
    //RedisDBRetryManager检查程序的线程数
    protected int redisRetryCheckThreads = 10;
    // 数据源配置
//    protected DataSourceConfig dsConfig;
//    protected DataSourceConfig readDSConfig;
    // 固定重试服务地址
    protected String fixAddress;
    //重试限速
    protected int retryLimitSpeed = 0;
    //重试线程数
    protected int remoteRetryLimitThread = 30;
    //是否远程加载RetryCount
    protected boolean remoteLoadRetryCount = true;

    public String getRetryLeaderPath() {
        return retryLeaderPath;
    }

    public void setRetryLeaderPath(String retryLeaderPath) {
        if (retryLeaderPath != null && !retryLeaderPath.isEmpty()) {
            this.retryLeaderPath = retryLeaderPath;
        }
    }

    public RetryManagerListener getRetryManagerListener() {
        return retryManagerListener;
    }

    public void setRetryManagerListener(RetryManagerListener retryManagerListener) {
        this.retryManagerListener = retryManagerListener;
    }

    public int getRetryQueueSize() {
        return retryQueueSize;
    }

    public void setRetryQueueSize(int retryQueueSize) {
        if (retryQueueSize > 0) {
            this.retryQueueSize = retryQueueSize;
        }
    }

    public int getRetryFetchSize() {
        return retryFetchSize;
    }

    public void setRetryFetchSize(int retryFetchSize) {
        if (retryFetchSize > 0) {
            this.retryFetchSize = retryFetchSize;
        }
    }

    public int getRetryFetchThreads() {
        return retryFetchThreads;
    }

    public void setRetryFetchThreads(int retryFetchThreads) {
        if (retryFetchThreads > 0) {
            this.retryFetchThreads = retryFetchThreads;
        }
    }

    public int getRetryFetchInterval() {
        return retryFetchInterval;
    }

    public void setRetryFetchInterval(int retryFetchInterval) {
        if (retryFetchInterval > 0) {
            this.retryFetchInterval = retryFetchInterval;
        }
    }

    public int getRetryClearInterval() {
        return retryClearInterval;
    }

    public void setRetryClearInterval(int retryClearInterval) {
        if (retryClearInterval > 0) {
            this.retryClearInterval = retryClearInterval;
        }
    }

    public int getRetryTables() {
        return retryTables;
    }

    public void setRetryTables(int retryTables) {
        if (retryTables > 0) {
            this.retryTables = retryTables;
        }
    }

    public int getRetryTimeout() {
        return retryTimeout;
    }

    public void setRetryTimeout(int retryTimeout) {
        if (retryTimeout > 0) {
            this.retryTimeout = retryTimeout;
        }
    }

    public long getStatTimeWindowSize() {
        return statTimeWindowSize;
    }

    public void setStatTimeWindowSize(long statTimeWindowSize) {
        if(statTimeWindowSize > 1000) {
            this.statTimeWindowSize = statTimeWindowSize;
        }
    }

    public int getLimitTimes() {
        return limitTimes;
    }

    public void setLimitTimes(int limitTimes) {
        if(limitTimes > 0) {
            this.limitTimes = limitTimes;
        }
    }

    public int getRetryLimitSpeed() {
        return retryLimitSpeed;
    }

    public void setRetryLimitSpeed(int retryLimitSpeed) {
        if(retryLimitSpeed > 0) {
            this.retryLimitSpeed = retryLimitSpeed;
        }
    }

    public int getRemoteRetryLimitThread() {
        return remoteRetryLimitThread;
    }

    public void setRemoteRetryLimitThread(int remoteRetryLimitThread) {
        if(remoteRetryLimitThread > 0) {
            this.remoteRetryLimitThread = remoteRetryLimitThread;
        }
    }

    public boolean isRemoteLoadRetryCount() {
        return remoteLoadRetryCount;
    }

    public void setRemoteLoadRetryCount(boolean remoteLoadRetryCount) {
        this.remoteLoadRetryCount = remoteLoadRetryCount;
    }

    public int getRedisRetryCheckThreads() {
        return redisRetryCheckThreads;
    }

    public void setRedisRetryCheckThreads(int redisRetryCheckThreads) {
        this.redisRetryCheckThreads = redisRetryCheckThreads;
    }

//    public DataSourceConfig getDsConfig() {
//        return dsConfig;
//    }
//
//    public void setDsConfig(DataSourceConfig dsConfig) {
//        this.dsConfig = dsConfig;
//    }
//
//    public DataSourceConfig getReadDSConfig() {
//        return readDSConfig;
//    }
//
//    public void setReadDSConfig(DataSourceConfig readDSConfig) {
//        this.readDSConfig = readDSConfig;
//    }

    public String getFixAddress() {
        return fixAddress;
    }

    public void setFixAddress(String fixAddress) {
        this.fixAddress = fixAddress;
    }

    public Set<String> getFixAddresses() {
        Set<String> fixAddresses = null;
        String fixAddress = getFixAddress();
        if (fixAddress != null && !fixAddress.isEmpty()) {
            fixAddresses = new HashSet<String>();
            String[] parts = fixAddress.split("[,;]");
            for (String part : parts) {
                fixAddresses.add(part);
            }
        }
        return fixAddresses;
    }



    @Override
    public void onEvent(ContextEvent event) {
        if (event == null) {
            return;
        }
        String key = event.getKey();
        if (BrokerContext.RETRY_SERVER_QUEUE_SIZE.equals(key)) {
            setRetryQueueSize(event.getPositive(retryQueueSize));
        } else if (BrokerContext.RETRY_SERVER_FETCH_SIZE.equals(key)) {
            setRetryFetchSize(event.getPositive(retryFetchSize));
        } else if (BrokerContext.RETRY_SERVER_FETCH_THREADS.equals(key)) {
            setRetryFetchThreads(event.getPositive(retryFetchThreads));
        } else if (BrokerContext.RETRY_SERVER_FETCH_INTERVAL.equals(key)) {
            setRetryFetchInterval(event.getPositive(retryFetchInterval));
        } else if (BrokerContext.RETRY_SERVER_CLEAR_INTERVAL.equals(key)) {
            setRetryClearInterval(event.getPositive(retryClearInterval));
        } else if (BrokerContext.RETRY_SERVER_TIMEOUT.equals(key)) {
            setRetryTimeout(event.getPositive(retryTimeout));
        } else if(BrokerContext.RETRY_SERVER_STAT_TIME_WINDOW_SIZE.equals(key)){
            setStatTimeWindowSize(event.getPositive(statTimeWindowSize));
        } else if(BrokerContext.RETRY_SERVER_LIMIT_TIMES.equals(key)){
            setLimitTimes(event.getPositive(limitTimes));
        } else if(BrokerContext.RETRY_SERVER_LIMIT_SPEED.equals(key)){
            setRetryLimitSpeed(event.getPositive(retryLimitSpeed));
        } else if(BrokerContext.RETRY_REMOTE_LOAD_RETRY_COUNT.equals(key)){
            setRemoteLoadRetryCount(event.getBoolean(remoteLoadRetryCount));
        } else if(BrokerContext.RETRY_REMOTE_RETRY_LIMIT_THREAD.equals(key)){
            setRemoteRetryLimitThread(event.getPositive(remoteRetryLimitThread));
        }
    }
}
