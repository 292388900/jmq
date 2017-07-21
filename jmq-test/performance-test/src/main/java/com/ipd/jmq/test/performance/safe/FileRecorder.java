package com.ipd.jmq.test.performance.safe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by lining11 on 2017/5/9.
 */
public class FileRecorder {
    private static final Logger logger = LoggerFactory.getLogger(FileRecorder.class);


    private String producerPath = "/export/Data/jmq.safetest/produce";
    private String consumerPath = "/export/Data/jmq.safetest/consume";
//    private String producerPath = "D:\\export\\Data\\jmq.safetest\\produce";
//    private String consumerPath = "D:\\export\\Data\\jmq.safetest\\consume";

    Map<String,BufferedWriter> produceWriters = new HashMap<>();
    Map<String,BufferedWriter> consumeWriters = new HashMap<>();

    private int fileSize = 640000;

    ReentrantLock lock = new ReentrantLock();

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
    }

    public void addProduceRecord(String line) {

        BufferedWriter writer = getProduceBufferedReader(line);
        if (writer != null) {
            try {
                writer.write(line);
                writer.newLine();
                writer.flush();
            } catch (IOException e) {
                logger.error("Write Produce Line error!",e);
            }
        }else {
            logger.error("The writer is null!");
        }

    }

    public void addConsumeRecord(String line) {
        BufferedWriter writer = getConsumeBufferedReader(line);
        if (writer != null) {
            try {
                writer.write(line);
                writer.newLine();
                writer.flush();
            } catch (IOException e) {
                logger.error("Write Consume Line error!",e);
            }
        }else {
            logger.error("The writer is null!");
        }
    }

    public BufferedWriter getProduceBufferedReader(String line) {
        long seri = Long.parseLong(line);
        String key = (seri / fileSize) + "";
        BufferedWriter produceWriter = produceWriters.get(key);
        if (produceWriter == null) {
            try {
                lock.lock();
                if (produceWriter != null) {
                    return produceWriter;
                }
                try {
                    File path = new File(producerPath);
                    if (!path.exists()){
                        path.mkdirs();
                    }
                    File file = new File(producerPath,"produce" + key + ".txt");
                    if (!file.exists()){
                        file.createNewFile();
                    }
                    FileWriter writer = new FileWriter(file,true);
                    produceWriter = new BufferedWriter(writer);
                } catch (IOException e) {
                    logger.error("Create Produce Buffer Reader error!",e);
                }
            } finally {
                lock.unlock();
            }
        }
        return produceWriter;
    }

    public BufferedWriter getConsumeBufferedReader(String line) {
        long seri = Long.parseLong(line);
        String key = (seri / fileSize) + "";
        BufferedWriter consumeWriter = consumeWriters.get(key);
        if (consumeWriter == null) {
            try {
                lock.lock();
                consumeWriter = consumeWriters.get(key);
                if (consumeWriter != null) {
                    return consumeWriter;
                }
                try {
                    File file = new File(consumerPath);
                    if (!file.exists()){
                        file.mkdirs();
                    }
                    file = new File(consumerPath,"consume" + key + ".txt");
                    if (!file.exists()){
                        file.createNewFile();
                    }
                    FileWriter writer = new FileWriter(file,true);
                    consumeWriter = new BufferedWriter(writer);
                    consumeWriters.put(key,consumeWriter);
                } catch (IOException e) {
                    logger.error("Create Consume Buffer error!",e);
                }
            } finally {
                lock.unlock();
            }
        }
        return consumeWriter;
    }

}
