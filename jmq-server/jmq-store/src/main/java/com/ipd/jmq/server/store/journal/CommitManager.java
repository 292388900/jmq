package com.ipd.jmq.server.store.journal;

import com.ipd.jmq.toolkit.concurrent.RingBuffer;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 组提交管理器，用于日志提交
 */
public class CommitManager extends Service {
    protected static Logger logger = LoggerFactory.getLogger(CommitManager.class);
    // 数据目录
    protected AppendDirectory directory;
    // 队列配置
    protected QueueConfig config;
    // 文件统计
    protected FileStat fileStat;
    // 双缓冲区
    protected RingBuffer events;
    // 致命异常
    protected JournalException.FatalException fatal;

    public CommitManager(AppendDirectory directory, QueueConfig config, FileStat fileStat) {
        Preconditions.checkArgument(directory != null, "directory can not be null");
        Preconditions.checkArgument(config != null, "config can not be null");
        Preconditions.checkArgument(fileStat != null, "fileStat can not be null");
        this.directory = directory;
        this.config = config;
        this.fileStat = fileStat;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        events = new RingBuffer(config.getSize(), new CommitHandler(), CommitManager.class.getSimpleName());
        events.start();
        logger.info("commit manager is started.");
    }

    @Override
    protected void doStop() {
        super.doStop();
        events.stop(20 * 1000);
        events = null;
        fatal = null;
        logger.info("commit manager is stopped.");
    }

    /**
     * 添加数据
     *
     * @param request 数据
     * @throws IOException
     */
    public void add(final CommitRequest request) throws IOException {
        if (request == null) {
            return;
        } else if (!isStarted()) {
            throw JournalException.IllegalStateException.build("commit manager is stopped.");
        } else if (fatal != null) {
            fileStat.error();
            throw JournalException.FatalException.build("fatal error occurred. self-protection", fatal);
        } else if (!events.add(request, config.getTimeout())) {
            fileStat.error();
            throw JournalException.TimeoutStateException
                    .build(String.format("enqueue timeout. %d(ms)", config.getTimeout()));
        }
    }

    /**
     * 刷盘
     *
     * @return 返回刷盘的位置
     */
    protected long flush() {
        if (fatal == null) {
            // 出现致命错误后自我保护
            AppendFile file = directory.last();
            if (file != null) {
                try {
                    // 刷盘
                    file.flush();
                    // 刷盘的最后位置
                    return file.id() + file.flushPosition();
                } catch (IOException e) {
                    fatal = JournalException.FatalException
                            .build(String.format("flush file error. %s", file.file().getPath()), e);
                }
            } else {
                fatal = JournalException.CreateFileException.build("there is not any journal file.");
            }
        }
        return -1;
    }

    /**
     * 处理器
     */
    protected class CommitHandler implements RingBuffer.EventHandler {
        @Override
        public void onEvent(final Object[] elements) throws Exception {
            // 刷盘获取最后的位置
            long position = flush();
            CommitRequest request;
            CountDownLatch latch;
            Period period;
            JournalException.FatalException fe = fatal;
            // 遍历请求
            for (Object element : elements) {
                request = (CommitRequest) element;
                if (position >= 0 && position < request.getOffset()) {
                    // 刷盘最后位置在该请求位置之前
                    logger.warn(String.format("flush offset %d is before request %d.", position, request.getOffset()));
                }
                // 设置异常
                request.setException(fe);
                // 统计结束
                period = request.getWrite();
                period.end();
                // 放开栅栏
                latch = request.getLatch();
                if (latch != null) {
                    // 触发等待刷盘的线程
                    latch.countDown();
                }
                // 统计信息
                if (fe != null) {
                    // 转换成微妙
                    fileStat.success(1, request.size(), (int) period.getTimeUnit().toMicros(period.time()));
                } else {
                    fileStat.error();
                }
            }
        }

        @Override
        public void onException(final Throwable e) {
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

