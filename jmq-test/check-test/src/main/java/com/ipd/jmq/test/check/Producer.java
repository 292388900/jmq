package com.ipd.jmq.test.check;

import com.ipd.jmq.test.check.kafka.producer.KafkaProducer;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Producer {
	private static final Logger logger = Logger.getLogger("producer");
	private static final int INCREMENT_VALUE = 20;
	private ExecutorService executorService;
	private int producerNumber = 1;
	private long totalNumber = 0L;
	private static String clientType = Util.get("clientType", "kafka");

	public Producer(String clientType){
		this.clientType = clientType;
	}

	protected void stop() {
		if (executorService != null && !executorService.isShutdown()) {
			executorService.shutdownNow();
		}
		executorService = null;
	}

	protected class ProducerTask implements Runnable {
		private String flag;
		protected long incrementValue = 0L;
		private long result = 0L;

		public ProducerTask(String flag) {
			this.flag = flag;
		}

		@Override
		public synchronized void run() {
			ClientProducer producer = null;
			if(clientType.equals("kafka")){
				producer = new KafkaProducer();
			}
			if (producer != null) {
				try {
					producer.open();
				} catch (Exception e) {
					logger.error("connect fail!", e);
					try {
					    producer.close();
                    } catch (Exception ignored) {
                    }
				}
				logger.info(flag + ": It's time to produce messages!");
				String body = Util.get("message_body", "");
				long i = getId();
				while (!Thread.interrupted() && !Util.IS_STOP.get()) {
					try {
						if (body.isEmpty()) {
							producer.send(flag + "=" + i + "");
						} else {
							producer.send(flag + "=" + i + "&" + body);
						}
						logger.info(flag + " Send " + String.valueOf(i) + " message");
						i = getId();
					} catch (Exception e) {
						logger.error(e.getMessage(),e);
						logger.info(flag + " Send error! " + String.valueOf(i));
					}
				}
				//close(null, session, producer);
			}
		}

		public synchronized long getId() {
			if (result == incrementValue) {
				incrementValue = getIncrementValue();
				result = incrementValue - INCREMENT_VALUE;
			}
			return result++;
		}

	}

	protected synchronized long getIncrementValue() {
		totalNumber = totalNumber + INCREMENT_VALUE;
		return totalNumber;
	}

	public void start(int producerNumber) {
		this.producerNumber = producerNumber;
		if (executorService == null) {
			executorService = Executors.newFixedThreadPool(this.producerNumber);
		}

		for (int i = 1; i <= producerNumber; i++) {
			executorService.execute(new ProducerTask("TD" + i));
		}
		try {
			while (!Thread.interrupted() && !Util.IS_STOP.get()) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					stop();
					return;
				}
			}
		} catch (Exception e) {
			stop();
			logger.error(e.getMessage(), e);
		}
		stop();
	}
	
	
	public static void main(String[] args){
		new Producer(clientType).start(3);
	}

}