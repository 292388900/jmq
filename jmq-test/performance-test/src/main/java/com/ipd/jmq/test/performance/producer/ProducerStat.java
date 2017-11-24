package com.ipd.jmq.test.performance.producer;

import com.ipd.jmq.test.performance.ProducerMain;
import com.ipd.jmq.test.performance.producer.jmq.JMQProducer;
import com.ipd.jmq.test.performance.producer.kafka.KafkaProducer;

import com.ipd.jmq.toolkit.stat.TPStat;
import com.ipd.jmq.toolkit.stat.TPStatBuffer;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhangkepeng on 17-1-6.
 */
public class ProducerStat {
    private final static Logger logger = LoggerFactory.getLogger(ProducerStat.class);

    private ExecutorService executor = null;
    private int threadNum = 1;
    private Producer producer;
    private String type = "";
    private long interval = 1000;

    // 统计变量
    public static TPStatBuffer tpStatBuffer = new TPStatBuffer();
    public static AtomicLong num = new AtomicLong(0);
    public static AtomicLong lastNum= new AtomicLong(0);
    public static AtomicLong time = new AtomicLong(0);
    public static AtomicLong lastTime= new AtomicLong(0);
    public static AtomicInteger errs = new AtomicInteger(0);

    public ProducerStat(String type, long interval) {
        this.type = type;
        this.interval = interval;
    }

    Thread t = new Thread() {
        public void run(){
            while(!Thread.interrupted()) {
                time.set(SystemClock.now());
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                long curNum = num.get();
                long curTime = time.get();

                lastNum.set(curNum);
                lastTime.set(curTime);

                TPStat stat = tpStatBuffer.getTPStat();
                tpStatBuffer.clear();
                String info = String.format("[%s]total num:%d, err:%d, success:%d, error:%d, tps:%d, tp99=%d, tp999=%d, min=%d, avg=%d, max=%d",
                        type, num.get(), errs.get(), stat.getSuccess(), stat.getError(), stat.getTps(), stat.getTp99(), stat.getTp999(), stat.getMin(), stat.getAvg(), stat.getMax());
                logger.info(info);
            }

        }
    };

    public void stat(final String produceType, final int threadSize, final String topics, final int bodyLen, final int maxMsgCount, boolean isAsync, final long maxUsedLimited, final long timeLimited) {
        String[] topicArr = topics.split(",");
        threadNum = threadSize*topicArr.length;
        executor = Executors.newFixedThreadPool(threadNum);
        final StringBuilder body = createText(bodyLen);

        t.start();
        producer = getProducer(produceType, isAsync);
        if (producer == null) {
            logger.error("can't find producer, produce type is kafka or jmq");
            return;
        }

        for (String curTopic : topicArr) {
            final String topic = curTopic;
            for (int i = 0; i < threadSize; i++) {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        SpeedLimiter speedLimiter = new SpeedLimiter(maxUsedLimited, timeLimited);
                        while (!Thread.interrupted()) {
                            long start = System.nanoTime();
                            try {
//                                speedLimiter.acquire();
                                producer.send(topic, body.toString());
//                                speedLimiter.release();
                                long duration = System.nanoTime() - start;
                                tpStatBuffer.success(1, bodyLen, (int) (duration / 1000000));
                                num.incrementAndGet();
                                if (maxMsgCount > 0 && num.get() >= maxMsgCount) {
                                    break;
                                }
                            } catch (Exception e) {
                                logger.error("", e);
//                                tpStatBuffer.error(1);
                                errs.incrementAndGet();
                            }
//                            logger.info("send message count: " + num);
                        }
                    }
                });
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

    private StringBuilder createText(int len){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append("a");
        }
        return sb;
    }

    public static Producer getProducer(String produceType, boolean isAsync){
        Producer sendProducer = null;
        if (produceType.equals(ProducerMain.PRODUCER_KAFKA)) {
            sendProducer = createKafkaProducer(isAsync);
        } else if (produceType.equals(ProducerMain.PRODUCER_JMQ)) {
            sendProducer = createJMQProducer();
        }
        return sendProducer;
    }

    public static Producer createKafkaProducer(boolean isAsync){
        KafkaProducer producer = new KafkaProducer(isAsync);
        producer.init();
        return producer;
    }

    public static Producer createJMQProducer() {
        JMQProducer producer = new JMQProducer();
        producer.init();
        return producer;
    }
}
