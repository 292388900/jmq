package com.ipd.jmq.server.store;

import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.toolkit.concurrent.EventBus;
import com.ipd.jmq.toolkit.concurrent.RingBuffer;
import com.ipd.jmq.toolkit.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 实时创建队列服务，每新增一条消息，立即入队
 */
public class DispatchService extends Service {
    private static Logger logger = LoggerFactory.getLogger(DispatchService.class);
    //当前分配的最大日志offset
    private long journalOffset;
    //队列管理器
    private QueueManager queueManager;
    //RingBuffer
    private RingBuffer events;
    //存储配置
    private StoreConfig config;
    // 存储监听器
    private EventBus<StoreEvent> eventManager;

    public DispatchService(StoreConfig config, QueueManager queueManager, EventBus<StoreEvent> eventManager) {
        if (queueManager == null) {
            throw new IllegalArgumentException("queueManager can not be null");
        }
        this.queueManager = queueManager;
        this.config = config;
        this.eventManager = eventManager;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        events = new RingBuffer(config.getDispatchQueueSize(), new DispatchHandler(), "DispatchService");
        events.start();
        logger.info("dispatch service is started.");
    }

    @Override
    protected void doStop() {
        events.stop(20 * 1000);
        events = null;
        super.doStop();
        logger.info("dispatch service is stopped.");
    }

    /**
     * 添加数据
     *
     * @param request 数据
     * @param timeout 超时
     * @throws java.lang.Exception
     */
    public void add(final DispatchRequest request, final int timeout) throws Exception {
        if (!isStarted()) {
            throw new JMQException("dispatch service is stopped.", JMQCode.CN_SERVICE_NOT_AVAILABLE.getCode());
        }
        boolean flag = events.add(request, timeout);
        if (!flag) {
            throw new JMQException(JMQCode.SE_ENQUEUE_SLOW);
        }
    }

    /**
     * 是否还有剩余数据
     *
     * @return 还有剩余数据标示
     */
    public boolean hasRemaining() {
        return events.readable() > 0;
    }

    public long getJournalOffset() {
        return journalOffset;
    }

    /**
     * 分发数据到队列
     *
     * @param request 请求
     * @throws Exception
     */
    protected void dispatch(final DispatchRequest request) throws Exception {
        if(logger.isTraceEnabled()){
            logger.trace("pre-create index:"+request);
        }
        //添加到队列
        ConsumeQueue queue = queueManager.getAndCreateQueue(request.getTopic(), request.getQueueId());
        queue.append(request);
        //记录最大日志文件位置
        journalOffset = request.getJournalOffset() + request.getSize();
    }

    /**
     * 派发处理器
     */
    protected class DispatchHandler implements RingBuffer.EventHandler {

        private long count = 0;

        @Override
        public void onEvent(final Object[] elements) throws Exception {
            DispatchRequest request;
            for (Object obj : elements) {
                request = (DispatchRequest) obj;
                dispatch(request);

                if (logger.isDebugEnabled()) {
                    count++;
                    if (count % 50000 == 0) {
                        logger.debug("num:" + count + " append queue:" + request.toString());
                    }
                }

            }
        }

        @Override
        public void onException(Throwable e) {
            // 异常关闭服务
            if (isStarted()) {
                logger.error(e.getMessage(), e);
                // TODO 出现异常是否需要关闭服务
                stop();
                if (eventManager != null) {
                    eventManager.add(new StoreEvent(StoreEvent.EventType.EXCEPTION,
                            new JMQException(e.getMessage(), e, JMQCode.SE_FATAL_ERROR.getCode())));
                }
            }
        }

        @Override
        public int getBatchSize() {
            return 0;
        }

        @Override
        public long getInterval() {
            return 0;
        }
    }

}

