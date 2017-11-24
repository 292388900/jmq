package com.ipd.jmq.client.consumer.offset;

/**
 * Created by dingjun on 15-9-16.
 */
public interface OffsetManage {
    /**
     * 初始化
     */
    void start() throws Exception;

    /**
     * 关闭
     */
    void stop();

    /**
     * 获取消费位置,如果没有初始化则返回-1,由服务端决定消费位置
     * @param clientName 客户端名称
     * @param groupName jmq group name
     * @param topic 主题
     * @param queueId 队列Id
     * @return 消费位置
     */

    long getOffset(String clientName, String groupName, String topic, short queueId);

    /**
     * 重置消费位置
     * @param clientName 客户端
     * @param groupName jmq group name
     * @param topic 主题
     * @param queueId 队列Id
     * @param offset 消费位置
     * @return 重置后的消费位置
     */
    long resetOffset(String clientName, String groupName, String topic, short queueId, long offset, boolean flush);

    /**
     * 更新消费位置
     * @param clientName 客户端名称
     * @param groupName jmq group name
     * @param topic 主题
     * @param queueId 队列Id
     * @param offset 位置
     * @param flush 是否立即持久化
     * @param increaseOnly 是否只更新增大的offset
     * @return 更新后的消费位置
     */
    long updateOffset(String clientName, String groupName, String topic, short queueId, long offset, boolean flush, boolean increaseOnly);

    /**
     * 根据 topic/clientName 持久化
     * @param topic 主题
     * @param clientName 客户端名称
     */
    void flush(String topic, String clientName);

    /**
     * 全部持久化
     */
    void flush();
}
