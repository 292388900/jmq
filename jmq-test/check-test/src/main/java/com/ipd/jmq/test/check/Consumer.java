package com.ipd.jmq.test.check;

import com.ipd.jmq.test.check.kafka.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * 
 * @version 1.0.0
 */
public class Consumer {
	private static final Logger logger = LoggerFactory.getLogger("consumer");

	private static final String clientType = Util.get("clientType", "kafka");

	private ClientConsumer consumer;

    public Consumer(String clientType) {
        if (this.clientType.equals(clientType)) {
            consumer = new KafkaConsumer();
        }
    }

    public void stop() throws Exception {
        try {
            consumer.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new Exception(e.getMessage());
        }
    }



	public void start() throws Exception {
		try {
			consumer.open();
			while (!Thread.interrupted() && !Util.IS_STOP.get()) {
				consumer.consume();
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					stop();
					return;
				}
			}
		} catch (Exception e) {
			logger.error("",e);
			throw new Exception(e.getMessage());
		}
	}

	
	public static void main(String[] args){
		try {
			new Consumer(clientType).start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("",e);
		}
	}

}
