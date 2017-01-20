package com.ipd.jmq.server.broker.offset;

import java.io.File;

/**
 * 偏移量管理配置
 *
 * @author lindeqiang
 * @since 2016/9/5 8:39
 */
public class OffsetConfig {
    //数据目录
    private File dataDirectory;
    //消费位置文件
    private File offsetFile;
    //主题队列文件
    private File topicQueueFile;

    public OffsetConfig(File dataDirectory) {
        if (dataDirectory == null) {
            throw new IllegalArgumentException("dataDirectory can not be null");
        }
        if (dataDirectory.exists()) {
            if (!dataDirectory.isDirectory()) {
                throw new IllegalArgumentException(String.format("%s is not a directory", dataDirectory.getPath()));
            }
        } else {
            if (!dataDirectory.mkdirs()) {
                if (!dataDirectory.exists()) {
                    throw new IllegalArgumentException(
                            String.format("create directory %s error.", dataDirectory.getPath()));
                }
            }
        }
        if (!dataDirectory.canWrite()) {
            throw new IllegalArgumentException(String.format("%s can not be written", dataDirectory.getPath()));
        }
        if (!dataDirectory.canRead()) {
            throw new IllegalArgumentException(String.format("%s can not be read", dataDirectory.getPath()));
        }
        this.dataDirectory = dataDirectory;
        this.offsetFile = new File(dataDirectory, "offset");
        this.topicQueueFile = new File(dataDirectory, "queues");
    }

    public OffsetConfig(String file) {
        this(new File(file));
    }

    public File getOffsetFile() {
        return offsetFile;
    }

    public void setOffsetFile(File offsetFile) {
        this.offsetFile = offsetFile;
    }

    public File getTopicQueueFile() {
        return topicQueueFile;
    }

    public void setTopicQueueFile(File topicQueueFile) {
        this.topicQueueFile = topicQueueFile;
    }
}
