package com.ipd.jmq.test.performance;

import com.ipd.jmq.test.performance.producer.Producer;
import com.ipd.jmq.test.performance.producer.jmq.JMQProducer;
import com.ipd.jmq.test.performance.producer.kafka.KafkaProducer;
import com.ipd.jmq.toolkit.stat.TPStat;
import com.ipd.jmq.toolkit.stat.TPStatBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by dingjun on 15-7-24.
 */
public class ProduceStat {
    String type = "";
    long interval = 1000;
    public static TPStatBuffer tpStatBuffer = new TPStatBuffer();
    public static AtomicLong num = new AtomicLong(0);
    public static AtomicLong time= new AtomicLong(0);

    public ProduceStat(String type, long interval) {
        this.type = type;
        this.interval = interval;
    }

    private static Logger log = LoggerFactory.getLogger(ProduceStat.class);

    AtomicLong lastNum= new AtomicLong(0);
    AtomicLong lastTime= new AtomicLong(0);

    AtomicInteger errs = new AtomicInteger(0);


    ExecutorService executor = null;
    int threadNum = 1;

    Producer producer;

    Thread t = new Thread(){
        public void run(){
            while(!Thread.interrupted()) {
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                long curNum = num.get();
                long curTime = time.get();
                long count = curNum - lastNum.get();
                long totalTime = curTime - lastTime.get();

                lastNum.set(curNum);
                lastTime.set(curTime);

                TPStat stat = tpStatBuffer.getTPStat();
                tpStatBuffer.clear();
                String info = String.format("[%s]total num:%d, tps:%.2f - tps2:%.2f err:%d, tp99=%d, tp999=%d, min=%d, avg=%d, max=%d",
                        type, curNum, totalTime == 0 ? 0 : count * 1000.0 * 1000 * 1000 * threadNum / totalTime, count * 1000.0 / interval, errs.get(),
                        stat.getTp99(), stat.getTp999(), stat.getMin(), stat.getAvg(), stat.getMax());
                log.info(info);
                System.out.println(info);
            }

        }
    };

    public int getThreadNum() {
        return threadNum;
    }

    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }

    public Producer getProducer() {
        return producer;
    }

    public void setProducer(Producer producer) {
        this.producer = producer;
    }

    public void stat(final String produceType, final int connectModel, final int threadSize, final String topics, final int bodyLen, final int maxMsgCount, final String app, final boolean isAsync){
        String[] topicArr = topics.split(",");
        threadNum = threadSize*topicArr.length;
        executor = Executors.newFixedThreadPool(threadNum);
        final StringBuilder body = creatText(bodyLen);

        t.start();
        if(connectModel==1){
            producer = getProducer(produceType, isAsync);
        }else{
          producer=null;
        }
        final CountDownLatch latch = new CountDownLatch(threadNum);
        for (String curTopic : topicArr) {
            final String topic = curTopic;

            for (int i = 0; i < threadSize; i++) {

                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        Producer sendProducer = producer;
                        if (connectModel == 2) {
                            sendProducer = getProducer(produceType, isAsync);
                        }
                        try {
                            System.out.println("TOPIC:"+topic);
                            if(produceType.equals("amq")){
                                //拿配置
                                sendProducer.send(topic,"");
                            }
                            latch.await();
                        } catch (Exception e) {
                            log.error("", e);
                            errs.incrementAndGet();
                        }

                        while (!Thread.interrupted()) {
                            try {
                                sendProducer.send(topic, body.toString());
                                if (maxMsgCount > 0 && num.get() >= maxMsgCount) {
                                    break;
                                }
                            } catch (Exception e) {
                                log.error("", e);
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

    private StringBuilder creatText(int len){
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<len;i++){
            sb.append("a");
        }
        return sb;
    }

    public static Producer getProducer(String produceType, boolean isAsync){
        Producer sendProducer;
        if (produceType.equals("jmq")) {
            sendProducer = createJMQProducer();
        }else{
            sendProducer = createKafkaProducer(isAsync);
        }
        return sendProducer;
    }

    public static Producer createKafkaProducer(boolean isAsync){
        KafkaProducer producer = new KafkaProducer(isAsync);
        producer.init();
        return producer;
    }

    public static Producer createJMQProducer(){
        JMQProducer producer = new JMQProducer();
        producer.init();
        return producer;
    }
}
