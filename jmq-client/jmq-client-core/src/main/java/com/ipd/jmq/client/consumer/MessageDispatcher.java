package com.ipd.jmq.client.consumer;


import com.ipd.jmq.common.cluster.TopicConfig;

import java.util.List;

/**
 * 消息派发器接口.
 *
 * @author lindeqiang
 * @since 14-5-8 下午2:04
 */
public interface MessageDispatcher {

    /**
     * 派发消息
     *
     * @param dispatches  待应答消息
     * @param topicConfig 主题配置
     */
    boolean dispatch(List<MessageDispatch> dispatches, TopicConfig topicConfig);
}
