package com.ipd.jmq.client.consumer.offset;

import com.ipd.jmq.toolkit.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhangkepeng on 15-8-24.
 */
public class FileLocalOffsetStore extends Service {
    // 日志
    private static final Logger logger = LoggerFactory.getLogger(FileLocalOffsetStore.class);
    // 客户端实例名称
    private String clientName;
    // 存储路径
    private String storePath;
    // 主题名称
    private String topic;
    // 偏移量表
    private ConcurrentHashMap<LocalMessageQueue, AtomicLong> offsetTable = new ConcurrentHashMap<LocalMessageQueue, AtomicLong>();
    // 文件开启标识
    private AtomicBoolean isOpen = new AtomicBoolean(false);

    public FileLocalOffsetStore(String rootPath, String topic, String clientName) {

        this.clientName = clientName;
        this.topic = topic;
        this.storePath = rootPath + File.separator + this.topic + //
                File.separator + this.clientName + File.separator + "offsets";
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public String getClientName() {
        return clientName;
    }

    private void setOffsetTable(ConcurrentHashMap<LocalMessageQueue, AtomicLong> offsetTable) {
        this.offsetTable = offsetTable;
    }

    public ConcurrentHashMap<LocalMessageQueue, AtomicLong> getOffsetTable() {
        return offsetTable;
    }

    public String getStorePath() {
        return storePath;
    }

    public void setStorePath(String storePath) {
        this.storePath = storePath;
    }

    public void doStart() throws Exception{
        load();
    }

    public void doStop(){
        persist();
        isOpen.set(false);
    }

    private void load() throws Exception {
        //防止多进程并发
        try {
            FileOperate.lockFile(storePath.substring(0, storePath.lastIndexOf(File.separator)) + File.separator + "lock");
        } catch (Exception e) {
            throw new RuntimeException("lock file failed! Maybe other process is fetching the file lock");
        }
        LocalOffsetSerializeWrapper localOffsetSerializeWrapper = this.readLocalOffset();
        if (localOffsetSerializeWrapper != null && localOffsetSerializeWrapper.getOffsetTable() != null) {
            offsetTable.putAll(localOffsetSerializeWrapper.getOffsetTable());

            for (LocalMessageQueue mq : localOffsetSerializeWrapper.getOffsetTable().keySet()) {
                AtomicLong offset = localOffsetSerializeWrapper.getOffsetTable().get(mq);
                logger.info("load consumer's offset, {} {} {}",//
                        this.clientName,//
                        mq,//
                        offset.get());
            }
        }
        isOpen.compareAndSet(false, true);
    }


    public void updateOffset(LocalMessageQueue mq, long offset, boolean increaseOnly) {
        if (mq != null) {
            AtomicLong offsetOld = this.offsetTable.get(mq);
            if (null == offsetOld) {
                offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
            }

            if (null != offsetOld) {
                if (increaseOnly) {
                    compareAndIncreaseOnly(offsetOld, offset);
                } else {
                    offsetOld.set(offset);
                }
            }
        }
    }

    public long readOffset(final LocalMessageQueue mq, final ReadOffsetType type) {
        if (mq != null) {
            switch (type) {
                case MEMORY_FIRST_THEN_STORE:
                case READ_FROM_MEMORY: {
                    AtomicLong offset = this.offsetTable.get(mq);
                    if (offset != null) {
                        return offset.get();
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                        return -1;
                    }
                }
                case READ_FROM_STORE: {
                    LocalOffsetSerializeWrapper localOffsetSerializeWrapper;
                    try {
                        localOffsetSerializeWrapper = this.readLocalOffset();
                    } catch (Exception e) {
                        return -1;
                    }
                    if (localOffsetSerializeWrapper != null && localOffsetSerializeWrapper.getOffsetTable() != null) {
                        AtomicLong offset = localOffsetSerializeWrapper.getOffsetTable().get(mq);
                        if (offset != null) {
                            this.updateOffset(mq, offset.get(), false);
                            return offset.get();
                        }
                    }
                }
                default:
                    break;
            }
        }

        return -1;
    }

    public void persist() {
        if (!isOpen.get()) {
            return;
        }
        if (this.offsetTable.isEmpty()) {
            return;
        }
        String time;
        LocalOffsetSerializeWrapper offsetSerializeWrapper = new LocalOffsetSerializeWrapper();
        for (Map.Entry<LocalMessageQueue,AtomicLong> entry : this.offsetTable.entrySet()) {
            time = getCurrentTime();
            entry.getKey().setTime(time);
            offsetSerializeWrapper.getOffsetTable().put(entry.getKey(), entry.getValue());
        }
        persistObject(offsetSerializeWrapper);
    }

    private String getCurrentTime() {
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateString = formatter.format(currentTime);

        return dateString;
    }

    private synchronized void persistObject(LocalOffsetSerializeWrapper offsetSerializeWrapper) {
        if (offsetSerializeWrapper != null) {
            try {
                String serString = offsetSerializeWrapper.objectToJsonString(true);
                if (serString != null) {
                    try {
                        FileOperate.string2File(serString, this.storePath);
                    } catch (IOException e) {
                        logger.error("persist offset exception. May be more than one same clientInstance, whose path is  " + this.storePath, e);
                        throw e;
                    }
                }
            } catch (Exception e) {
                logger.error("Object serialize error", e);
            }
        }
    }


    private LocalOffsetSerializeWrapper readLocalOffset() throws Exception {
        LocalOffsetSerializeWrapper localOffsetSerializeWrapper = null;
        String content = null;
        content = FileOperate.file2String(this.storePath);
        if (null == content || content.length() == 0) {
            return this.readLocalOffsetBak();
        } else {
            try {
                localOffsetSerializeWrapper = readLocalOffsetSerializeWrapper(content);
            } catch (Exception e) {
                logger.error("read local offset serialize wrapper exception", e);
            }
            return localOffsetSerializeWrapper;
        }
    }


    private LocalOffsetSerializeWrapper readLocalOffsetBak() throws Exception {
        String content = FileOperate.file2String(this.storePath + ".bak");
        LocalOffsetSerializeWrapper localOffsetSerializeWrapper = null;
        try {
            localOffsetSerializeWrapper = readLocalOffsetSerializeWrapper(content);
        } catch (Exception e) {
            logger.error("read local offset serialize wrapper Exception", e);
        }
        return localOffsetSerializeWrapper;
    }

    private LocalOffsetSerializeWrapper readLocalOffsetSerializeWrapper(String content) throws Exception{
        LocalOffsetSerializeWrapper localOffsetSerializeWrapper = null;
        if (content != null && content.length() > 0) {
            try {
                localOffsetSerializeWrapper =
                        LocalOffsetSerializeWrapper.jsonStringToObject(content, LocalOffsetSerializeWrapper.class);
            } catch (Exception e) {
                throw new Exception("read local offset exception, and try to correct", e);
            }

        }
        return localOffsetSerializeWrapper;
    }


    public void removeOffset(LocalMessageQueue mq) {
    }


    public Map<LocalMessageQueue, Long> cloneOffsetTable(String topic) {
        Map<LocalMessageQueue, Long> cloneOffsetTable = new HashMap<LocalMessageQueue, Long>();
        Iterator<LocalMessageQueue> iterator = this.offsetTable.keySet().iterator();
        while (iterator.hasNext()) {
            LocalMessageQueue mq = iterator.next();
            if (!isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, this.offsetTable.get(mq).get());
        }
        return cloneOffsetTable;
    }

    public void lockFile() throws Exception{
        String storePath = this.storePath;
        try {
            FileOperate.lockFile(storePath);
        } catch (Exception e) {
            logger.error(storePath + "file is writing or reading");
            throw e;
        }

    }

    public static boolean compareAndIncreaseOnly(final AtomicLong target, final long value) {
        long prev = target.get();
        while (value > prev) {
            boolean updated = target.compareAndSet(prev, value);
            if (updated)
                return true;

            prev = target.get();
        }

        return false;
    }

    public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if ((Character.isWhitespace(str.charAt(i)) == false)) {
                return false;
            }
        }
        return true;
    }

    public boolean hasOffsetTableMq(LocalMessageQueue mq) {
        if (mq != null) {
            if (this.offsetTable.containsKey(mq)) {
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FileLocalOffsetStore that = (FileLocalOffsetStore) o;

        if (clientName != null ? !clientName.equals(that.clientName) : that.clientName != null) {
            return false;
        }
        if (topic != null ? !topic.equals(that.topic) : that.topic != null) {
            return false;
        }
        if (!storePath.equals(that.storePath)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = storePath != null ? storePath.hashCode() : 0;
        result = 31 * result + (topic != null ? topic.hashCode() : 0);
        result = 31 * result + (clientName != null ? clientName.hashCode() : 0);
        return result;
    }
}
