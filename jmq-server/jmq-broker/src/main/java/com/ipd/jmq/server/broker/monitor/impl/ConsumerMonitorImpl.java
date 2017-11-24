package com.ipd.jmq.server.broker.monitor.impl;

import com.alibaba.fastjson.JSON;
import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.message.MessageQueue;
import com.ipd.jmq.common.model.OffsetInfo;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.netty.NettyDecoder;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
import com.ipd.jmq.server.broker.monitor.api.ConsumerMonitor;
import com.ipd.jmq.server.broker.offset.Offset;
import com.ipd.jmq.server.broker.offset.QueueOffset;
import com.ipd.jmq.server.broker.offset.TopicOffset;
import com.ipd.jmq.server.store.ConsumeQueue;
import com.ipd.jmq.server.store.GetResult;
import com.ipd.jmq.server.store.Store;
import com.ipd.jmq.toolkit.buffer.RByteBuffer;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by zhangkepeng on 16-11-30.
 */
public class ConsumerMonitorImpl implements ConsumerMonitor{
    private static final Logger logger = LoggerFactory.getLogger(ConsumerMonitorImpl.class);

    // 派发服务
    protected DispatchService dispatchService;
    // 集群管理
    protected ClusterManager clusterManager;
    // 存储
    protected Store store;

    public ConsumerMonitorImpl(Builder builder) {
        this.store = builder.store;
        this.clusterManager = builder.clusterManager;
        this.dispatchService = builder.dispatchService;
    }

    /**
     * 增加消费者
     *
     * @param topic 主题
     * @param app   应用
     */
    @Override
    public void subscribe(String topic, String app) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("topic can not be empty");
        }

        if (app == null || app.trim().isEmpty()) {
            throw new IllegalArgumentException("app can not be empty");
        }
        dispatchService.subscribe(topic, app);
    }

    /**
     * 删除消费者
     *
     * @param topic 主题
     * @param app   应用
     */
    @Override
    public void unsubscribe(String topic, String app) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("topic can not be empty");
        }

        if (app == null || app.trim().isEmpty()) {
            throw new IllegalArgumentException("app can not be empty");
        }

        dispatchService.unSubscribe(topic, app);
    }

    /**
     * 获取服务端消息
     * @param topic
     * @param app
     * @param count
     * @return
     */
    @Override
    public List<BrokerMessage> viewMessage(String topic, String app, int count) {

        List<BrokerMessage> messages = new ArrayList<BrokerMessage>(count);

        try {
            TopicConfig.ConsumerPolicy policy = clusterManager.checkReadable(topic, app);
            if (policy == null) {
                logger.error("--viewMessage Null policy--" + topic + "--" + app + "--" + count);
                return messages;
            }

            TopicOffset topicOffset = dispatchService.getOffset(topic);
            if (topicOffset == null) {
                logger.error("--viewMessage Null topicOffset--" + topic + "--" + app + "--" + count);
                return messages;
            }


            messages = getMessage(topic, app, count, false);
            if (messages.size() == 0) {
                //没有积压,则拉取已消费过的消息
                messages = getMessage(topic, app, count, true);
            }

        } catch (Exception e) {
            logger.error("--viewMessage error--" + topic + "--" + app + "--" + count, e);
        }

        return messages;
    }

    private List<BrokerMessage> getMessage(String topic, String app, int count, boolean getConsumedMessage) {

        List<BrokerMessage> messages = new ArrayList<BrokerMessage>(count);

        TopicOffset topicOffset = dispatchService.getOffset(topic);
        if (topicOffset == null) {
            logger.error("--viewMessage Null topicOffset--" + topic + "--" + app + "--" + count);
            return messages;
        }


        int remain = count;
        Map<Integer, QueueOffset> queues = topicOffset.getOffsets();
        /**
         *均摊在每个队列上的条数,没有拉取一条
         */
        int perQueueCount = count / queues.size();
        if (perQueueCount == 0) {
            perQueueCount = 1;
        }

        for (Integer queueId : queues.keySet()) {

            long offset = dispatchService.getOffset(topic, queueId, app);
            if (getConsumedMessage) {
                //拉取已消费的消息,需要重新计算已消费消息offset
                long newOffset = (perQueueCount * ConsumeQueue.CQ_RECORD_SIZE);
                if (newOffset <= offset) {
                    //防止负数
                    offset = offset - newOffset;
                }
            }


            GetResult result = null;
            try {
                result = store.getMessage(topic, queueId, offset, (short) remain, NettyDecoder.FRAME_MAX_SIZE - 4);
            } catch (Exception e) {
                logger.error("--store.getMessage error--" + topic + "--" + queueId + "--offset--" + offset, e);
            }

            /**
             * 无数据
             */
            if (result == null || result.getBuffers() == null) {
                continue;
            }


            /**
             * 序列化消息
             */
            for (RByteBuffer buffer : result.getBuffers()) {
                try {
                    BrokerMessage message = Serializer.readBrokerMessage(Unpooled.wrappedBuffer(buffer.getBuffer()));

                    Map<String, String> attr = message.getAttributes();
                    if (attr == null) {
                        attr = new HashMap<String, String>();
                        message.setAttributes(attr);
                    }
                    attr.put("pending", Boolean.valueOf(getConsumedMessage).toString());
                    messages.add(message);
                } catch (Exception e) {
                    logger.error("--Serializer.readBrokerMessage error--", e);
                }
            }


            try {
                result.release();
            } catch (Exception e) {
                logger.error("--result.release error--" + topic + "--queueId--" + queueId);
            }

            /**
             * 条数够了,停止拉取
             */
            int size = result.getBuffers().size();
            if ((remain - size) > 0) {
                remain = remain - size;
            } else {
                break;
            }
        }


        return messages;
    }

    @Override
    public OffsetInfo getOffsetInfo(String topic, short queueId, String app) {
        OffsetInfo offsetInfo = new OffsetInfo();
        if (topic == null) {
            logger.error("topic is equal to null");
            return null;
        }
        if (queueId < 1 || queueId > MessageQueue.MAX_NORMAL_QUEUE) {
            logger.error("queueId is invalid");
            return null;
        }
        long minOffset;
        long maxOffset;
        long minOffsetIdTimestamp;
        long maxOffsetIdTimestamp;

        try {
            minOffset = store.getMinOffset(topic, queueId);
            maxOffset = store.getMaxOffset(topic, queueId);
            minOffsetIdTimestamp = store.getMinOffsetTimestamp(topic, queueId);
            maxOffsetIdTimestamp = store.getMaxOffsetTimestamp(topic, queueId);
            offsetInfo.setMinOffsetForValue(minOffset);
            offsetInfo.setMinOffsetForDate(minOffsetIdTimestamp);
            offsetInfo.setMaxOffsetForValue(maxOffset);
            offsetInfo.setMaxOffsetForDate(maxOffsetIdTimestamp);
        } catch (JMQException e) {
            throw new RuntimeException(e);
        }
        return offsetInfo;
    }

    @Override
    public void resetAckOffset(String topic, short queueId, String app, long offsetOrTime, boolean isOffset) throws
            JMQException {
        if (!isOffset) {
            Offset offset = dispatchService.getUnSequenceOffset(topic, app, queueId);
            if (offset == null) {
                logger.warn("Reset Ack offset by time failure,time=" + offsetOrTime);
                return;
            } else {
                offsetOrTime = store.getQueueOffsetByTimestamp(topic, queueId, app, offsetOrTime, offset.getAckOffset().get());
            }
        }
        dispatchService.resetAckOffset(topic, queueId, app, offsetOrTime);
    }

    @Override
    public String getNoAckBlocks() {
        return JSON.toJSONString(dispatchService.getNoAckBlocks());
    }

    @Override
    public List<String> getLocations(final String topic, final String app) {
        return dispatchService.getLocations(topic, app);
    }

    /**
     * 强制释放location
     */
    @Override
    public boolean cleanExpire(final String topic, final String app, final short queueId) {
        return dispatchService.cleanExpire(topic, app, queueId);
    }

    @Override
    public void clearRetryCache(String topic, String app) {
        //TODO
    }

    @Override
    public BlockingQueue<BrokerMessage> viewCachedRetry(String topic, String app) {
        // TODO
        return new ArrayBlockingQueue<BrokerMessage>(0);
    }

    @Override
    public void reloadRetryCache(String topic, String app) {
        // TODO
    }

    @Override
    public boolean isRetryLeader(String topic, String app) {
        //TODO
        return false;
    }

    public static class Builder {
        // 派发服务
        protected DispatchService dispatchService;
        // 集群管理
        protected ClusterManager clusterManager;
        // 存储
        protected Store store;

        public Builder(Store store, ClusterManager clusterManager, DispatchService dispatchService) {
            this.store = store;
            this.clusterManager = clusterManager;
            this.dispatchService = dispatchService;
        }

        public ConsumerMonitor build() {
            return new ConsumerMonitorImpl(this);
        }
    }
}
