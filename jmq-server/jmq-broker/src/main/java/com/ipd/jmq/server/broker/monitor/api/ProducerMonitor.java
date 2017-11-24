package com.ipd.jmq.server.broker.monitor.api;

/**
 * Created by zhangkepeng on 16-11-30.
 */
public interface ProducerMonitor {

    /**
     * 发送消息
     *
     * @param topic      主题
     * @param app        应用
     * @param body       内容
     * @param businessId 业务ID
     */
    void sendMessage(String topic, String app, String body, String businessId);
}
