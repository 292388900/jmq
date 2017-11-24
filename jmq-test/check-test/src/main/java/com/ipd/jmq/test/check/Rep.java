package com.ipd.jmq.test.check;

import org.apache.kahadb.util.Sequence;
import org.apache.kahadb.util.SequenceSet;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class Rep {

	private static final Logger logger = Logger.getLogger("checkLog");
	private static final String PATH = Util.get("log_path", "/export/Logs/check");
	private static final String BAK_PATH = PATH + File.separatorChar + "bak";


	private static ExecutorService consumerService = Executors.newSingleThreadExecutor();
	private static ExecutorService producerService = Executors.newSingleThreadExecutor();
	private static ScheduledExecutorService checkService = Executors.newScheduledThreadPool(1);

	final static String isProducer = Util.get("isProducer", "true");
	final static String isConsumer = Util.get("isConsumer", "true");
	final static String isCheck = Util.get("isCheck", "true");
	final static int stopPort = Integer.parseInt(Util.get("stopPort", "9991"));
	final static int checkInterval = Integer.parseInt(Util.get("check_interval", "5"));
	final static int producers = Integer.parseInt(Util.get("producers", "1"));
	final static String clientType = Util.get("clientType", "jmq");

	private static CheckTask task = new Rep().new CheckTask();

	public static void main(String[] args) throws Exception {
		ServerSocket server = null;
		BufferedReader in = null;
		PrintWriter out = null;
		try {
			// start consumer
			if (isConsumer.equals("true")) {
				consumerService.execute(new Runnable() {
					@Override
					public void run() {
						try {
							new Consumer(clientType).start();
						} catch (Exception e) {
							logger.error(e.getMessage(), e);
						}
					}
				});
			}
			// start producer
			if (isProducer.equals("true")) {
				producerService.execute(new Runnable() {
					@Override
					public void run() {
						new Producer(clientType).start(producers);
					}
				});
			}

			if (isCheck.equals("true")) {
				checkService.scheduleWithFixedDelay(task, 1, checkInterval, TimeUnit.MINUTES);
			}

			// socket

			server = new ServerSocket(stopPort);
			Socket client = server.accept();
			in = new BufferedReader(new InputStreamReader(client.getInputStream()));
			out = new PrintWriter(client.getOutputStream());
			String command = in.readLine();
			if ("stop".equals(command)) {
				stop();
				out.print("stop finish!");
				out.flush();
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (server != null) {
				server.close();
			}
			if (in != null) {
				in.close();
			}
			if (out != null) {
				out.close();
			}
		}
	}

	public static void stop() {
		try {
			Util.IS_STOP.set(true);
			logger.info("stop check application.......");
			// stop producer
			logger.info("stop producer.......");
			producerService.shutdownNow();
			Thread.sleep(2000);
			// stop consumer
			logger.info("stop consumer.......");
			consumerService.shutdownNow();
			logger.info("stop reboot.......");
			// handle log immediately
			checkService.execute(task);
			Thread.sleep(10000);
			int unHandFilesNum = 0;
			// check if files are finished or not
			do {
				File[] unHandFiles = getUnHandleFile();
				unHandFilesNum = unHandFiles.length;
				if (unHandFilesNum > 0) {
					StringBuilder sb = new StringBuilder();
					for (File file : unHandFiles) {
						sb.append(file.getName()).append(";");
					}
					logger.info("Please wait to handle these files. " + sb.toString());
					Thread.sleep(10000);
				}

			} while (unHandFilesNum != 0);
			logger.info("stop check.......");
			checkService.shutdownNow();
			// handle consumer.log
			task.setConsumerFile(new File(PATH + File.separatorChar + "consumer.log"));
			ExecutorService es = Executors.newSingleThreadExecutor();
			es.execute(task);
			Thread.sleep(10000);
			es.shutdown();
		} catch (InterruptedException e) {
		} finally {

		}
	}

	private static File[] getUnHandleFile() {
		File file = new File(PATH);
		File[] unHandleFiles = file.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				if (pathname.getName().startsWith("consumer.log.")) {
					return true;
				}
				return false;
			}
		});
		Arrays.sort(unHandleFiles);
		return unHandleFiles;
	}

	protected class CheckTask implements Runnable {
		private SequenceSet messageSequence = new SequenceSet();
		private File consumerFile;

		public void setConsumerFile(File consumerFile) {
			this.consumerFile = consumerFile;
		}

		@Override
		public synchronized void run() {
			File[] files = null;
			if (consumerFile == null) {
				files = getUnHandleFile();
				if (files.length < 4) {
					logger.info("There were " + files.length
							+ " files in last five minutes, which may have some exceptions. please check out!");
				}
			} else {
				files = new File[] { consumerFile };
			}

			for (File file : files) {
				try {
					logger.info("It's ready to handle file:" + file.getName());
					int lastMessage = handleFile(file, true);
					checkList(messageSequence, lastMessage);
					moveFile(file);
					logger.info("File:" + file.getName() + " has already finished!");
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				}
			}
			// move producer log
			moveProduceFiles();
		}

		private int handleFile(File file, boolean isCheck) throws Exception {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			int lastMessage = 0;
			while (line != null && !line.isEmpty()) {
				String[] lineMsg = line.split("@");
				if (lineMsg.length == 2) {
					String[] bodyAndMessageId = lineMsg[1].split("#");
					String counter = bodyAndMessageId[0];
					int message = 0;
					try {
						message = Integer.parseInt(counter);
						messageSequence.add(message);
						if (lastMessage < message) {
							lastMessage = message;
						}
					} catch (Exception e) {
						logger.error("message form is invalid! message : " + line, e);
						line = br.readLine();
						continue;
					}

				} else {
					logger.error("message form is invalid! message : " + line);
				}
				line = br.readLine();
			}
			br.close();
			return lastMessage;
		}

		private void checkList(SequenceSet messageSequence, int lastMessage) throws Exception {
			List<Sequence> last = messageSequence.getMissing(0, lastMessage);
			for (Iterator<Sequence> it = last.iterator(); it.hasNext();) {
				Sequence s = it.next();
				logger.info("message may be lost! message: " + s.getFirst() + "-" + s.getLast());
			}
		}

		private void moveFile(File file) {
			File bak = new File(BAK_PATH);
			if (!bak.exists()) {
				bak.mkdir();
			}
			File dest = new File(bak.getPath() + File.separatorChar + file.getName());
			if (file.renameTo(dest)) {
				file.delete();
			}
		}

		private void moveProduceFiles() {
			File file = new File(PATH);
			File[] produceFiles = file.listFiles(new FileFilter() {

				@Override
				public boolean accept(File pathname) {
					if (pathname.getName().startsWith("producer.log.")) {
						return true;
					}
					return false;
				}
			});
			for (File produce : produceFiles) {
				moveFile(produce);
			}
		}
	}

}
