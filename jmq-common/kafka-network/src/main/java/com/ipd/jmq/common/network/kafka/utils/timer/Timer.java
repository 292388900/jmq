package com.ipd.jmq.common.network.kafka.utils.timer;

import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ThreadSafe
public class Timer {
    private final static Logger logger = LoggerFactory.getLogger(Timer.class);

    private final DelayQueue<TimerTaskList> delayQueue = new DelayQueue<TimerTaskList>();
    private final TimingWheel timingWheel;
    private final ExecutorService taskExecutor;
    private final AtomicInteger taskCounter = new AtomicInteger(0);
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();

    public Timer(ExecutorService taskExecutor) {
        this(taskExecutor, 1, 20, SystemClock.now());
    }

    public Timer(ExecutorService taskExecutor, int tickMs, int wheelSize, long startMs) {
        Preconditions.checkArgument(taskExecutor != null, "ExecutorService can't be null");
        this.taskExecutor = taskExecutor;
        this.timingWheel = new TimingWheel(tickMs, wheelSize, startMs, taskCounter, delayQueue);
    }

    /**
     * Add a new task to this executor. It will be executed after the task's delay
     * (beginning from the time of submission)
     * @param timerTask the task to add
     */
    public void add(TimerTask timerTask) {
        readLock.lock();
        try {
            addTimerTaskEntry(new TimerTaskList.TimerTaskEntry(timerTask));
        } finally {
            readLock.unlock();
        }
    }


    protected void addTimerTaskEntry(TimerTaskList.TimerTaskEntry timerTaskEntry) {
        if (!timingWheel.add(timerTaskEntry)) {
            // Already expired or cancelled
            if (!timerTaskEntry.cancelled())
                taskExecutor.submit(timerTaskEntry.timerTask);
        }
    }

    /**
     * Advance the internal clock, executing any tasks whose expiration has been
     * reached within the duration of the passed timeout.
     * @param timeoutMs
     * @return whether or not any tasks were executed
     */
    public boolean advanceClock(long timeoutMs) {
        TimerTaskList bucket = null;
        try {
            bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("interrupted when poll TimerTaskList element");
        }
        if (bucket != null) {
            writeLock.lock();
            try {
                while (bucket != null) {
                    timingWheel.advanceClock(bucket.getExpiration());
                    bucket.flush(this);
                    bucket = delayQueue.poll();
                }
            } finally {
                writeLock.unlock();
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Get the number of tasks pending execution
     * @return the number of tasks
     */
    public int size() {
        return taskCounter.get();
    }
}
