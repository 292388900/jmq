package com.ipd.jmq.server.broker.retry;

import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.context.ContextManager;

/**
 * 重试管理器监听器
 */
public interface RetryManagerListener {

    /**
     * 重试管理器变化事件
     *
     * @param event 变更事件
     */
    void onEvent(RetryManagerEvent event);


    /**
     * 重试管理器变更事件
     */
    public static class RetryManagerEvent {
        // 重试管理器类型
        private RetryManagerType type;
        // 重试管理器
        private RetryManager manager;
        // 集群管理器
        private ClusterManager clusterManager;
        // 上下文管理器
        private ContextManager contextManager;

        public RetryManagerEvent(RetryManagerType type, RetryManager manager, ClusterManager clusterManager,
                                 ContextManager contextManager) {
            this.type = type;
            this.manager = manager;
            this.clusterManager = clusterManager;
            this.contextManager = contextManager;
        }

        public RetryManagerType getType() {
            return type;
        }

        public RetryManager getManager() {
            return manager;
        }

        public ClusterManager getClusterManager() {
            return clusterManager;
        }

        public ContextManager getContextManager() {
            return contextManager;
        }
    }

}
