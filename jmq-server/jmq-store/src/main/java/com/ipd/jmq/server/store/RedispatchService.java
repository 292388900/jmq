package com.ipd.jmq.server.store;

import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.exception.JMQOffsetException;
import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.toolkit.buffer.RByteBuffer;
import com.ipd.jmq.toolkit.concurrent.EventBus;
import com.ipd.jmq.toolkit.concurrent.RingBuffer;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.time.SystemClock;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 从节点异步入队服务，从数据文件异步读取到队列
 */
public class RedispatchService extends Service {
    private static Logger logger = LoggerFactory.getLogger(RedispatchService.class);
    // 日志管理器
    private JournalManager journalManager;
    // 存储配置
    private StoreConfig storeConfig;
    // 当前可用的最大位置
    private long maxJournalOffset;
    // 创建队列的位置
    private long journalOffset = -1;
    // 派发服务
    private RingBuffer ringBufferService;
    // 异常监听器
    private EventBus<StoreEvent> eventManager;

    public RedispatchService(StoreConfig storeConfig, JournalManager journalManager,
                             EventBus<StoreEvent> eventManager) {
        if (journalManager == null) {
            throw new IllegalArgumentException("journalManager can not be null");
        }
        if (storeConfig == null) {
            throw new IllegalArgumentException("storeConfig can not be null");
        }
        this.journalManager = journalManager;
        this.storeConfig = storeConfig;
        this.eventManager = eventManager;
    }

    @Override
    protected void validate() throws Exception {
        super.validate();
        if (journalOffset < 0) {
            throw new IllegalStateException("journalOffset is invalid");
        }
    }

    @Override
    protected void doStart() throws Exception {
        ringBufferService =
                new RingBuffer(storeConfig.getRedispatchQueueSize(), new EnqueueTask(), "RedispatchService");
        ringBufferService.start();
        logger.info("redispatch service is started.");
    }

    @Override
    public void doStop() {
        journalOffset = -1;
        ringBufferService.stop();
        ringBufferService = null;
        logger.info("redispatch service is stopped.");
    }


    /**
     * 设置最大位置
     *
     * @param maxJournalOffset 最大位置
     */
    public void setMaxJournalOffset(long maxJournalOffset) {
        this.maxJournalOffset = maxJournalOffset;
        if (ringBufferService != null) {
            ringBufferService.add(maxJournalOffset, -1);
        }
    }

    /**
     * 设置日志起始位置
     *
     * @param journalOffset 日志起始位置
     */
    public void setJournalOffset(long journalOffset) {
        if (this.journalOffset < 0) {
            this.journalOffset = journalOffset;
        }
    }

    /**
     * 重新派发任务
     */
    protected class EnqueueTask implements RingBuffer.EventHandler {

        private long batchCounter = 0;
        private long messageCounter = 0;
        private long lastTime = 0;

        @Override
        public void onEvent(final Object[] elements) throws Exception {
            if (!isStarted()) {
                return;
            }
            if (elements != null && elements.length > 0) {
                for (Object element : elements) {
                    enqueue((Long) element);
                    if (logger.isInfoEnabled()) {
                        if ((++batchCounter) % 60000 == 0 || SystemClock.getInstance()
                                .now() - lastTime > 1000 * 60 * 5) {
                            batchCounter = 0;
                            lastTime = SystemClock.now();
                            logger.info("redispatch journal at offset:" + journalOffset);
                        }
                    }
                }
            } else if (journalOffset < maxJournalOffset) {
                enqueue(maxJournalOffset);
            }
        }

        /**
         * 入队
         *
         * @param maxOffset 最大日志位置
         */
        protected void enqueue(final long maxOffset) throws Exception {
            long minJournalOffset = journalManager.getMinOffset();
            if (journalOffset < minJournalOffset) {
                logger.warn("rest journalOffset[" + journalOffset + "] to journal min offset:" + minJournalOffset);
                journalOffset = minJournalOffset;
            }
            // 判断是否有需要建队列的数据
            while (isStarted() && journalOffset < maxOffset) {
                // 计算能读取的最大数据大小mayby exteed Integer.MAX_VALUE
                long maxLength = maxOffset - journalOffset;
                if (maxLength < JournalManager.MESSAGE_HEAD_LEN) {
                    // 数据不够
                    break;
                }
                // 计算当前文件从该位置开始还有多少剩余空间
                int length =
                        storeConfig.getJournalConfig().getDataSize() - (int) (journalOffset % storeConfig.getJournalConfig().getDataSize());
                if (length < JournalManager.MESSAGE_HEAD_LEN) {
                    // 没有足够的数据，说明达到文件末尾
                    journalOffset += length;
                    continue;
                }
                // 从文件读取头部数据
                RByteBuffer headBuf =
                        journalManager.getJournalMessage(journalOffset, JournalManager.MESSAGE_HEAD_LEN);
                int size = 0;
                int code = 0;
                try {
                    if (headBuf == null || headBuf.getBuffer() == null || headBuf
                            .remaining() < JournalManager.MESSAGE_HEAD_LEN) {
                        // 数据文件不存在，或实际读取的数据小于头部长度
                        logger.error(String.format("no journal data. offset:%d", journalOffset));
                        break;
                    }
                    // 消息头部记录的数据大小
                    size = headBuf.getInt();
                    // 消息类型
                    code = headBuf.getShort();
                } finally {
                    if (headBuf != null) {
                        headBuf.release();
                    }
                }
                if (size <= 0 || (code != JournalManager.MAGIC_BLANK_CODE && code != JournalManager.MAGIC_MSG_CODE)) {
                    logger.error(String.format("invalid journal header. offset:%d size:%d code:%d", journalOffset, size,
                            code));
                    throw new JMQOffsetException(journalOffset);
                }
                if (maxLength < size) {
                    // 数据不完整，等待复制数据
                    break;
                }

                if (code == JournalManager.MAGIC_BLANK_CODE) {
                    // 空白数据
                    journalOffset += size;
                } else {
                    // 消息
                    RByteBuffer msgBuf = journalManager.getJournalMessage(journalOffset, size);
                    try {
                        if (msgBuf == null || msgBuf.getBuffer() == null || msgBuf.remaining() < size) {
                            // 数据不完整，等待复制数据
                            break;
                        }
                        // 反序列化消息
                        BrokerMessage message =
                                Serializer.readBrokerMessage(Unpooled.wrappedBuffer(msgBuf.getBuffer()));
                        //防止ReferenceBuffer被引用，后续处理用不到
                        message.setBody((byte[])null);
                        message.setText(null);
                        if (logger.isDebugEnabled()) {
                            if (++messageCounter % 50000 == 0) {
                                logger.debug(String.format("enqueue offset:%d size=%d topic=%s", journalOffset, size,
                                        message.getTopic()));
                            }
                        }
                        // 分发消息
                        journalManager.redispatch(message);
                        journalOffset += size;
                    } finally {
                        if (msgBuf != null) {
                            msgBuf.release();
                        }
                    }
                }
            }
        }

        @Override
        public void onException(Throwable e) {
            if (isStarted()) {
                logger.error("encounter fatal error. will stop redispatch service.", e);
                // TODO 出现异常是否需要关闭服务
                stop();
                // 广播出去
                if (eventManager != null) {
                    eventManager.add(new StoreEvent(StoreEvent.EventType.EXCEPTION,
                            new JMQException(e.getMessage(), e, JMQCode.SE_FATAL_ERROR.getCode())));
                }
            }
        }

        @Override
        public int getBatchSize() {
            return 1;
        }

        @Override
        public long getInterval() {
            return 5000;
        }
    }
}