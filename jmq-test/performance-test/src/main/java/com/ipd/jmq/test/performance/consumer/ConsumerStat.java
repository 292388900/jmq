package com.ipd.jmq.test.performance.consumer;

import com.ipd.jmq.test.performance.ConsumerMain;
import com.ipd.jmq.test.performance.consumer.jmq.JMQConsumer;
import com.ipd.jmq.test.performance.consumer.kafka.KafkaConsumer;

import com.ipd.jmq.toolkit.stat.TPStatBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhangkepeng on 17-1-6.
 */
public class ConsumerStat {
    private final static Logger logger = LoggerFactory.getLogger(ConsumerStat.class);

    private ExecutorService executor = null;
    private int threadNum = 1;
    private Consumer consumer1;
    private String type = "";
    private long interval = 2000;

    // 统计变量
    public static TPStatBuffer tpStatBuffer = new TPStatBuffer();
    public static AtomicLong num = new AtomicLong(0);
    public static AtomicLong lastNum= new AtomicLong(0);
    public static AtomicLong time = new AtomicLong(0);
    public static AtomicLong lastTime= new AtomicLong(0);
    public static AtomicInteger errs = new AtomicInteger(0);
    private AtomicBoolean flag = new AtomicBoolean(true);

    public ConsumerStat(String type, long interval) {
        this.type = type;
        this.interval = interval;
    }

    Thread t = new Thread() {

        public void run(){
            while(!Thread.interrupted()) {
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (consumer1 != null) {
                    num.set(consumer1.getCountValue());
                }
                long curNum = num.get();
                long count = curNum - lastNum.get();
                time.set(System.nanoTime());
                long duration;
                if (flag.compareAndSet(true, false)) {
                    duration = System.nanoTime() - time.get();
                } else {
                    duration = time.get() - lastTime.get();
                }
                long curTime = time.get();

                lastNum.set(curNum);
                lastTime.set(curTime);

                /*TPStat stat = tpStatBuffer.getTPStat();
                tpStatBuffer.clear();
                String info = String.format("[%s]total num:%d, err:%d, success:%d, error:%d, tp99=%d, tp999=%d, min=%d, avg=%d, max=%d",
                        type, num.get(), errs.get(), stat.getSuccess(), stat.getError(),
                        stat.getTp99(), stat.getTp999(), stat.getMin(), stat.getAvg(), stat.getMax());*/

                String info2 = String.format("%d messages was consumed in %s ms,TPS=%s, total=%s !",
                        count, duration / 1000000.0, count * 1000000000.0 / duration, num.get());

                logger.info(info2);
            }

        }
    };

    public void stat(final String consumerType, final int threadSize, final String topics, final String app) {
        String[] topicArr = topics.split(",");
        threadNum = threadSize*topicArr.length;
        executor = Executors.newFixedThreadPool(threadNum);
        t.start();

        final CountDownLatch latch = new CountDownLatch(threadNum);
        for (final String curTopic : topicArr) {
            for (int i = 0; i < threadSize; i++) {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        /**
                         * kafka 0.9 consumer is not thread-safe
                         */
                        Consumer consumer = getConsumer(consumerType);
                        if (consumer instanceof KafkaConsumer) {
                            ((KafkaConsumer) consumer).setTopic(curTopic);
                        }
                        consumer.init();
                        if (consumer1 == null) {
                            consumer1 = consumer;
                        }
                        try {
                            latch.await();
                        } catch (InterruptedException e) {
                            logger.error(e.getMessage(), e);
                        }

                        if (consumer instanceof KafkaConsumer) {
                            while (!Thread.interrupted()) {
                                try {
                                    consumer.run();
                                    // count
                                    num.set(consumer.getCountValue());
                                } catch (Exception e) {
                                    logger.error("", e);
                                    //tpStatBuffer.error(1);
                                    errs.incrementAndGet();
                                }
                            }
                        }

                        if (consumer instanceof JMQConsumer) {
                            try {
                                consumer.run();
                                if (consumer1 == null) {
                                    consumer1 = consumer;
                                }
                                num.set(consumer.getCountValue());
                            } catch (Exception e) {
                                logger.error(e.getMessage(), e);
                                errs.incrementAndGet();
                            }
                        }

                    }
                });
                latch.countDown();
            }
        }

        executor.shutdown();
        while (!executor.isTerminated()){
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private Consumer getConsumer(String type) {
        Consumer consumer = null;
        if (type.equals(ConsumerMain.CONSUMER_HIGH_KAFKA)) {
            consumer = new KafkaConsumer();
        } else {
            consumer = new JMQConsumer();
        }
        return consumer;
    }
}
