package com.ipd.jmq.client.consumer.offset.impl;

import com.ipd.jmq.client.consumer.offset.*;
import com.ipd.jmq.toolkit.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import java.util.concurrent.*;

/**
 * Created by dingjun on 15-9-16.
 */
public class FileOffsetManage extends Service implements OffsetManage {
    private static ConcurrentMap<String, FileOffsetManage> offsetManageMap = new ConcurrentHashMap<String, FileOffsetManage>();
    // 持久化偏移量时间间隔
    private long persistConsumerOffsetInterval = 1000*5;
    /**
     $root
        |--offset
            |
            |----topic
                |
                |----clientName
                    |
                    |----offset
                    |----offset.bak
                    |----lock
     */
    private static final Logger logger = LoggerFactory.getLogger(FileOffsetManage.class);
    /**
     * 存储根目录
     */
    private String rootPath;

    private ConcurrentMap<ClientTopicInfo,FileLocalOffsetStore> offsets = new ConcurrentHashMap<ClientTopicInfo, FileLocalOffsetStore>();

    private Thread flushThread ;

    public FileOffsetManage(String rootPath){
        this.rootPath = rootPath;
    }

    public static FileOffsetManage getInstance(String path) throws Exception {
        if (path == null) {
            path = System.getProperty("user.home");
        }
        FileOffsetManage fileOffsetManage = offsetManageMap.get(path);
        if (fileOffsetManage == null) {
            fileOffsetManage = new FileOffsetManage(path);
            fileOffsetManage.start();
            FileOffsetManage old = offsetManageMap.putIfAbsent(path, fileOffsetManage);
            if (old != null) {
                fileOffsetManage.stop();
                fileOffsetManage = old;
            }
        }
        return fileOffsetManage;
    }

    public void doStart(){
        if (this.rootPath == null || this.rootPath.isEmpty()) {
            this.rootPath = System.getProperty("user.home");
        }

        startPersistentOffset();
    }

    public void doStop(){
        if (flushThread != null) {
            flushThread.interrupt();
        }
        flush();
    }

    private void startPersistentOffset() {
        flushThread = new Thread("file-offset-flush") {
            public void run() {
                while (!Thread.interrupted()) {
                    if (isStarted()) {
                        flush();
                    }
                    try {
                        Thread.sleep(persistConsumerOffsetInterval);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        };
        flushThread.setDaemon(true);
        flushThread.start();
    }

    private FileLocalOffsetStore createOffsetStore(String topic, String clientName){
        FileLocalOffsetStore offsetStore;
        File offsetDir = new File(this.rootPath, "offset");
        ClientTopicInfo key = new ClientTopicInfo(topic, clientName);
        offsetStore = offsets.get(key);
        if (offsetStore != null && offsetStore.isStarted()) {
            return offsetStore;
        }
        offsetStore = new FileLocalOffsetStore(offsetDir.getPath(), topic, clientName);
        try {
            offsetStore.start();
        } catch (Exception e) {
            throw new RuntimeException(String.format("load %s error", offsetStore.getStorePath()), e);
        }
        return offsetStore;
    }

    private FileLocalOffsetStore getOrCreateOffsetStore(String topic, String clientName) {
        ClientTopicInfo key = new ClientTopicInfo(topic, clientName);
        FileLocalOffsetStore store = offsets.get(key);
        if (store != null) {
            return store;
        }
        writeLock.lock();
        try {
            store = createOffsetStore(topic, clientName);
            FileLocalOffsetStore oldStore = offsets.putIfAbsent(key, store);
            if (oldStore != null) {
                store = oldStore;
            }
        } finally {
            writeLock.unlock();
        }
        return store;
    }

    @Override
    public long getOffset(String clientName, String groupName, String topic, short queueId) {
        FileLocalOffsetStore offsetStore = getOrCreateOffsetStore(topic, clientName);
        LocalMessageQueue messageQueue = new LocalMessageQueue(groupName, topic, clientName, queueId);
        return offsetStore.readOffset(messageQueue, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
    }

    @Override
    public long resetOffset(String clientName, String groupName, String topic, short queueId, long offset, boolean flush) {
        FileLocalOffsetStore offsetStore = getOrCreateOffsetStore(topic, clientName);
        LocalMessageQueue messageQueue = new LocalMessageQueue(groupName, topic, clientName, queueId);
        offsetStore.updateOffset(messageQueue, offset, false);
        if (flush) {
            offsetStore.persist();
        }
        return offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY);
    }

    @Override
    public long updateOffset(String clientName, String groupName, String topic, short queueId, long offset, boolean flush, boolean increaseOnly) {
        FileLocalOffsetStore offsetStore = getOrCreateOffsetStore(topic, clientName);
        LocalMessageQueue messageQueue = new LocalMessageQueue(groupName, topic, clientName, queueId);
        offsetStore.updateOffset(messageQueue, offset, increaseOnly);
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("%s, %d", messageQueue.toString(), offset));
        }
        if (flush) {
            offsetStore.persist();
        }
        return offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY);
    }
    @Override
    public void flush(String topic, String clientName) {
        if (!this.isStarted()) {
            return;
        }
        FileLocalOffsetStore offsetStore = getOrCreateOffsetStore(topic, clientName);
        offsetStore.persist();
    }

    @Override
    public synchronized void flush() {
        if (!this.isStarted()) {
            return;
        }
        for (FileLocalOffsetStore offsetStore : offsets.values()) {
            offsetStore.persist();
        }
    }

    public void setPersistConsumerOffsetInterval(long persistConsumerOffsetInterval) {
        if (persistConsumerOffsetInterval > 0) {
            this.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
        }
    }

    public class ClientTopicInfo {
        private String topic;
        private String clientName;

        public ClientTopicInfo(String topic, String clientName) {
            this.topic = topic;
            this.clientName = clientName;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getClientName() {
            return clientName;
        }

        public void setClientName(String clientName) {
            this.clientName = clientName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ClientTopicInfo that = (ClientTopicInfo) o;

            if (!clientName.equals(that.clientName)) return false;
            if (!topic.equals(that.topic)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = topic.hashCode();
            result = 31 * result + clientName.hashCode();
            return result;
        }
    }
}
