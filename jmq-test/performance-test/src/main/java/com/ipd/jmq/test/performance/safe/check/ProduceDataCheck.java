package com.ipd.jmq.test.performance.safe.check;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.logging.FileHandler;

/**
 * Created by lining11 on 2017/5/10.
 */
public class ProduceDataCheck {

    private static final Logger logger = LoggerFactory.getLogger(ProduceDataCheck.class);

    public int fileSize = 10;
    public int lineSize = 100;
    private String path = "/export/Data/jmq.safetest";

    private String folder = "temp";

    BufferedWriter recorder = null;

    private String sourceFolder = "produce";

    private int nextFileNum = 0;


    private ConcurrentMap<Long, BufferedWriter> writers = new ConcurrentHashMap<>();

    public void setSourceFolder(String sourceFolder) {
        this.sourceFolder = sourceFolder;
    }

    public int getLineSize() {
        return lineSize;
    }

    public void start() throws IOException, InterruptedException {
        File record = new File(path + File.separator + "record" );
        if (!record.exists()){
            record.mkdirs();
        }
        record = new File(record.getAbsolutePath() + File.separator + sourceFolder + ".txt");
        if (!record.exists()){
            record.createNewFile();
        }
        FileWriter fileWriter = new FileWriter(record);
        recorder = new BufferedWriter(fileWriter);
        new Thread() {
            @Override
            public void run() {
                File folderConsume = new File(path + File.separator + sourceFolder);

                while (true) {
                    if (!folderConsume.exists()) {
                        folderConsume.mkdirs();
                    }
                    File[] files = folderConsume.listFiles();
                    if (files.length > 5) {
                        File file = new File(path + File.separator + sourceFolder + File.separator + sourceFolder +nextFileNum + ".txt");
                        if (!file.exists()) {
//                            if (files.length > 5){
//                                List<Integer> fileNums = new ArrayList<Integer>();
//                                for (File f:files){
//                                    String fileName = f.getName();
//                                    String numStr = fileName.substring(sourceFolder.length(),fileName.indexOf("."));
//                                    Integer num = Integer.parseInt(numStr);
//                                    fileNums.add(num);
//                                }
//                                Collections.sort(fileNums);
//                                nextFileNum = fileNums.get(0);
//                            }
                            nextFileNum++;
                            continue;
                        }
                        try {
                            checkData(file);
                        } catch (Exception e) {
                            logger.error("checkData Error! ", e);
                            continue;
                        }
                        nextFileNum++;
                    } else {
                        try {
                            Thread.currentThread().sleep(1000);
                        } catch (InterruptedException e) {
                            logger.error("InterruptedException", e);
                        }
                    }

                }
            }
        }.start();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }

    public void setLineSize(int lineSize) {
        this.lineSize = lineSize;
        this.fileSize = lineSize * lineSize;
    }

    public void checkData(File file) throws IOException {
        boolean delete = true;

        if (file == null) {
            file = new File(path + File.separator + folder + File.separator + "0.txt");
        }
        AtomicReferenceArray<AtomicIntegerArray> timer = new AtomicReferenceArray<AtomicIntegerArray>(lineSize);
        FileReader fileReader = new FileReader(file);
        BufferedReader reader = new BufferedReader(fileReader);
        String serialStr = null;
        try {
            while ((serialStr = reader.readLine()) != null) {
                if ("".equals(serialStr)) {
                    continue;
                }
                long serial = Long.parseLong(serialStr);
                int index = (int) (serial % fileSize);
                int lineIndex = (index / lineSize);
                int veIndex = (index % lineSize);
                AtomicIntegerArray integerArray = timer.get(lineIndex);
                if (integerArray == null) {
                    integerArray = new AtomicIntegerArray(lineSize);
                    timer.set(lineIndex, integerArray);
                }
                integerArray.set(veIndex, index);
            }
        } finally {
            fileReader.close();
            reader.close();
        }

        logger.info("++++++++++Start Check!fileName:" + file.getName() + "++++++++++++++++++");
        for (int line = 0; line < lineSize; line++) {
            AtomicIntegerArray lineArray = timer.get(line);
            for (int column = 0; column < lineSize - 1; column++) {
                if (null == lineArray){
                    if (recorder != null) {
                        recorder.write(String.format("----------------fileName:%s,lineArray:%s", file.getName(), column + ""));
                        recorder.newLine();
                        recorder.flush();
                    } else {
                        logger.info(String.format("----------------fileName:%s,lineArray:%s", file.getName(), column + ""));
                    }
                    continue;
                }
                if (lineArray.get(column + 1) - lineArray.get(column) != 1) {
                    delete = false;
                    if (recorder != null) {
                        recorder.write(String.format("----------------fileName:%s,pre:%s,next:%s", file.getName(), lineArray.get(column) + "", lineArray.get(column + 1) + ""));
                        recorder.newLine();
                        recorder.flush();
                    } else {
                        logger.info(String.format("----------------fileName:%s,pre:%s,next:%s", file.getName(), lineArray.get(column) + "", lineArray.get(column + 1) + ""));
                    }
                }

            }
        }
        logger.info("++++++++++Complete Check!fileName:" + file.getName() + "++++++++++++++++++");
        if (delete) {
            file.delete();
            logger.info(String.format("File:%s is deleted!", file.getName()));
        } else {
            FileWriter fileWriter = null;
            BufferedWriter writer = null;
            try {
                File temtFile = new File(path + File.separator + folder );
                if (!temtFile.exists()){
                    temtFile.mkdirs();
                }
                temtFile = new File(temtFile.getAbsolutePath() + File.separator + file.getName() + ".bak");
                if (!temtFile.exists()){
                    temtFile.createNewFile();
                }
                fileWriter = new FileWriter(temtFile);
                writer = new BufferedWriter(fileWriter);

                fileReader = new FileReader(file);
                reader = new BufferedReader(fileReader);
                String lineStr = null;
                while ((lineStr = reader.readLine()) != null) {
                    writer.write(lineStr);
                    writer.newLine();
                    writer.flush();
                }

            } finally {
                fileReader.close();
                fileWriter.close();

                reader.close();
                writer.close();

                file.delete();

            }

        }

    }

}
