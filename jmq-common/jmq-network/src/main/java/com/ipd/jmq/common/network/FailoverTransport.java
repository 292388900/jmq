package com.ipd.jmq.common.network;


import com.ipd.jmq.toolkit.concurrent.EventListener;

/**
 * 故障切换传输接口
 */
public interface FailoverTransport extends Transport {

    /**
     * 不健康状态
     */
    void weak();

    /**
     * 获取连接状态
     *
     * @return 连接状态
     */
    FailoverState getState();

    /**
     * 添加事件监听器
     *
     * @param listener 监听器
     */
    void addListener(EventListener<FailoverState> listener);

    /**
     * 移除事件监听器
     *
     * @param listener 监听器
     */
    void removeListener(EventListener<FailoverState> listener);

    /**
     * 获取权重
     *
     * @return 权重
     */
    int getWeight();

}