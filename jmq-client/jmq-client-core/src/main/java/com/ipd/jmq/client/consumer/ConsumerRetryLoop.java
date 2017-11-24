package com.ipd.jmq.client.consumer;

import com.ipd.jmq.toolkit.retry.RetryPolicy;
import com.ipd.jmq.toolkit.time.SystemClock;
import java.util.concurrent.Callable;

/**
 * 重试
 */
public class ConsumerRetryLoop {

    /**
     * 循环重试
     *
     * @param retryPolicy 重试策略
     * @param proc        回调函数
     * @param <T>
     * @return 返回结果
     * @throws Exception
     */
    public static <T> T execute(RetryPolicy retryPolicy, RetryCallable<T> proc) throws Exception {
        if (proc == null) {
            throw new IllegalArgumentException("proc can not be null");
        }
        if (retryPolicy == null) {
            throw new IllegalArgumentException("retryPolicy can not be null");
        }
        int retryCount = 0;
        long startTime = SystemClock.now();
        while (proc.shouldContinue()) {
            try {
                return proc.call();
            } catch (Exception e) {
                if (proc.onException(e)) {
                    // 计算下次重试时间点
                    long now = SystemClock.now();
                    long time = retryPolicy.getTime(now, ++retryCount, startTime);
                    if (time <= 0) {
                        // 不在重试
                        throw e;
                    }
                    try {
                        // 休息一段时间
                        Thread.sleep(time - now);
                    } catch (InterruptedException ignored) {
                        throw e;
                    }
                } else {
                    throw e;
                }
            }
        }
        return null;
    }

    /**
     * 重试回调
     *
     * @param <T>
     */
    public static interface RetryCallable<T> extends Callable<T> {
        /**
         * 出现异常，是否要继续
         *
         * @param e 异常
         * @return <li>true 继续重试</li>
         * <li>false 退出重试</li>
         */
        boolean onException(Exception e);

        /**
         * 是否要继续
         *
         * @return 是否要继续重试
         */
        boolean shouldContinue();
    }

}