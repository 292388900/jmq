package com.ipd.jmq.server.store;

import com.ipd.jmq.server.store.journal.FileStat;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.service.ServiceThread;
import com.ipd.jmq.toolkit.stat.TPStat;
import com.ipd.jmq.toolkit.stat.TPStatBuffer;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.concurrent.locks.Lock;

/**
 * 检查点刷盘管理器
 * Created by hexiaofeng on 15-6-17.
 */
public class FlushManager extends Service {
    private static Logger logger = LoggerFactory.getLogger(FlushManager.class);
    // 配置
    protected StoreConfig config;
    // 检查点
    protected CheckPoint checkPoint;
    // 日志管理器
    protected JournalManager journalManager;
    // 队列管理器
    protected QueueManager queueManager;
    // 派发服务
    protected DispatchService dispatchService;
    // 存储
    protected Service store;
    // 检查点刷盘线程
    protected Thread thread;

    public FlushManager(StoreConfig config, CheckPoint checkPoint, JournalManager journalManager,
                        QueueManager queueManager, DispatchService dispatchService, Service store) {

        Preconditions.checkArgument(config != null, "config can not be null");
        Preconditions.checkArgument(checkPoint != null, "checkPoint can not be null");
        Preconditions.checkArgument(journalManager != null, "journalManager can not be null");
        Preconditions.checkArgument(queueManager != null, "queueManager can not be null");
        Preconditions.checkArgument(dispatchService != null, "dispatchService can not be null");
        Preconditions.checkArgument(store != null, "store can not be null");

        this.config = config;
        this.checkPoint = checkPoint;
        this.journalManager = journalManager;
        this.queueManager = queueManager;
        this.dispatchService = dispatchService;

        this.store = store;
    }

    @Override
    protected void doStart() throws Exception {
        // 构造检查点线程并启动
        thread = new Thread(new CheckPointTask(store), "JMQ_SERVER_CHECK_POINT");
        thread.start();

        super.doStart();
    }

    @Override
    protected void doStop() {
        // 关闭线程
        Close.close(thread);
        thread = null;
        super.doStop();
    }

    /**
     * 检查点刷盘
     *
     * @throws IOException
     */
    protected void flush() throws IOException {
        // 服务已经关闭
        if (!isStarted() || !store.isStarted()) {
            return;
        }
        //先保存日志文件的位置到临时变量，再刷新queue文件，然后再写检查点，避免刷queue时有内容写入
        long recoverOffset = dispatchService.getJournalOffset();
        // 队列刷盘
        queueManager.flush();

        // 存储服务加锁，防止关闭
        Lock lock = ((JMQStore) store).getLock();
        lock.lock();
        try {
            // 再次确认是否已经关闭了
            if (!isStarted() || !store.isStarted()) {
                return;
            }
            // 检查点刷盘
            if (recoverOffset > 0) {
                //如果没有发消息，重启过服务，可能会被设置为0
                checkPoint.setRecoverOffset(recoverOffset);
            }
            checkPoint.setJournalOffset(journalManager.getMaxOffset());
            checkPoint.flush();
        } finally {
            lock.unlock();
        }
    }


    /**
     * 检查点刷盘任务
     */
    protected class CheckPointTask extends ServiceThread {
        // 计数器
        protected long counter = 0;

        public CheckPointTask(Service parent) {
            super(parent);
        }

        @Override
        protected void execute() throws Exception {
            counter++;
            // 判断存储服务是否就绪
            if (parent.isReady()) {
                // 刷盘
                long start = SystemClock.now();
                flush();
                long queueFlushDuration = SystemClock.now() - start;

                if (counter % 20 == 0) {
                    // 统计数据
                    FileStat journalFileStat = journalManager.getFileStat();
                    FileStat queueFileStat = queueManager.getFileStat();
                    int journalFiles = journalFileStat.getAppendFiles();
                    long journalFileSize = journalFileStat.getAppendFileLength();
                    int queueFiles = queueFileStat.getAppendFiles();
                    long queueFileSize = queueFileStat.getAppendFileLength();
                    FileStat.FileStatSlice statSlice = journalFileStat.slice();
                    TPStatBuffer buffer = statSlice.getWriteStat();
                    TPStat stat = buffer.getTPStat();
                    double diskSpeed = calcDiskSpeed(stat);
                    logger.info(String.format(
                            "flush checkpoint success,use time:%s. \n{\"journals\":%s,\"journalSize\":%s," +
                                    "\"journalCaches\":%s,\"journalCacheSize\":%s,\"queues\":%s," +
                                    "\"queueSize\":%s,\"queueCaches\":%s,\"queueCacheSize\":%s," +
                                    "\"diskSpeed\":%.2f,\"diskAvgTime\":%s}",
                            queueFlushDuration, journalFiles, journalFileSize,
                            journalFileStat.getMappedFiles(), journalFileStat.getMappedMemory(), queueFiles,
                            queueFileSize, queueFileStat.getMappedFiles(), queueFileStat.getMappedMemory(), diskSpeed,
                            stat.getAvg()));
                    counter = 0;
                } else {
                    logger.info(String.format("Checkpoint, flush queue success, takes %d ms", queueFlushDuration));
                }
            } else {
                logger.info("store is not ready at checkpoint.");
            }
        }

        private double calcDiskSpeed(TPStat stat){
            long size = stat.getSize();
            long time = stat.getTime();
            double diskSpeed = 0.0;
            if (time > 0) {
                BigDecimal bg = new BigDecimal(size * 1000000000.0 / time);
                diskSpeed = bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
            }
            return diskSpeed;
        }

        @Override
        public long getInterval() {
            return config.getCheckPoint();
        }

        @Override
        public boolean onException(Throwable e) {
            logger.error("flush queue error.", e);
            return true;
        }
    }
}
