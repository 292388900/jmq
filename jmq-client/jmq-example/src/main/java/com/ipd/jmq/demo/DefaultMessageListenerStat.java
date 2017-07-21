package com.ipd.jmq.demo;

import com.ipd.jmq.client.consumer.MessageListener;
import com.ipd.jmq.common.message.Message;
import org.apache.log4j.Logger;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 消息监听器.
 */
public class DefaultMessageListenerStat implements MessageListener {
    private static BitSet consumedMessage = new BitSet(100000);
    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private static final Logger logger = Logger.getLogger(DefaultMessageListenerStat.class);

    static {
        Thread thread = new SummaryService();
        thread.start();
    }

    /**
     * 消费方法。注意: 消费不成功请抛出异常，MQ会自动重试
     *
     * @param messages
     * @throws Exception
     */
    @Override
    public void onMessage(List<Message> messages) throws Exception {
        if (messages == null || messages.isEmpty()) {
            return;
        }
      //throw new RuntimeException("test");

        for (Message message : messages) {
            String text = message.getText();
            int msg = Integer.valueOf(text);
            lock.readLock().lock();
            try {
                if (consumedMessage.get(msg)) {
                    logger.warn(String.format("Duplicate message:%s", msg));
                } else {
                    consumedMessage.set(msg, true);
                }
            } finally {
                lock.readLock().unlock();
            }
        }
    }


    static class SummaryService extends Thread {
        @Override
        public void run() {
            logger.info("Start summary consumer!");
            while (true) {
                lock.writeLock().lock();
                BitSet set = null;
                try {
                    set = (BitSet) consumedMessage.clone();

                } finally {
                    lock.writeLock().unlock();
                }

                if (set != null) {
                    logger.info(String.format("set.size=%s, set.length=%s", set.size(), set.length()));

                    for (int i = 0, j = 0; i < set.size() && j < set.length(); i++) {
                        if (set.get(i)) {
                            j++;
                        } else {
                            logger.warn(String.format("Message %s may be lost", i));
                        }
                    }
                }
                try {
                    Thread.sleep(1000 * 30 * 1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                logger.info("Start summary consumer!");
            }

        }
    }
}
