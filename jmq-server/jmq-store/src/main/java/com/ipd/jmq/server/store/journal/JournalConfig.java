package com.ipd.jmq.server.store.journal;

import com.ipd.jmq.toolkit.io.Files;
import com.ipd.jmq.toolkit.validate.annotation.NotEmpty;
import com.ipd.jmq.toolkit.validate.annotation.Range;

/**
 * 镜像文件配置
 * Created by hexiaofeng on 16-7-12.
 */
public class JournalConfig {
    // 数据文件后缀，不包括"."
    @NotEmpty
    private String suffix;
    // 文件大小
    @Range(min = 1, max = Integer.MAX_VALUE)
    private int dataSize;
    // 数据的最大缓存空间(不包括文件头)
    private long cacheSize = -1;
    //缓存新生代比率(保留最热点的文件，超过则启动检查程序卸载)
    @Range(min = 1, max = 100)
    private int cacheNewRatio = 40;
    //缓存最大比率(超过该值，则不能缓存，可以设置为0不启用缓存)
    @Range(min = 0, max = 100)
    private int cacheMaxRatio = 95;
    //缓存强制收回的比率阀值(超过则按照优先级强制收回，队列文件数也可以尽量保留少)
    @Range(min = 1, max = 100)
    private int cacheEvictThreshold = 90;
    //缓存一次强制收回的最小比率
    @Range(min = 1, max = 100)
    private int cacheEvictMinRatio = 10;
    //每个数据目录最近的热点文件数量
    @Range(min = 0, max = Integer.MAX_VALUE)
    private int cacheFiles = 3;
    //缓存最大空闲时间，超过则会被移除到待清理队列
    @Range(min = 1, max = Integer.MAX_VALUE)
    private int cacheMaxIdle = 1000 * 60 * 10;
    //检查缓存时间间隔
    @Range(min = 1, max = Integer.MAX_VALUE)
    private int cacheCheckInterval = 1000 * 2;
    //缓存延时回收的时间，为0则立即释放
    @Range(min = 0, max = Integer.MAX_VALUE)
    private int cacheEvictLatency = 1000 * 10;
    //缓存存在内存泄露时间
    @Range(min = 1, max = Integer.MAX_VALUE)
    private int cacheLeakTime = 1000 * 180;

    public String getSuffix() {
        return suffix;
    }

    public void setSuffix(String suffix) {
        this.suffix = Files.suffix(suffix);
    }

    public int getDataSize() {
        return dataSize;
    }

    public void setDataSize(int dataSize) {
        this.dataSize = dataSize;
    }

    public long getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
    }

    public int getCacheNewRatio() {
        return cacheNewRatio;
    }

    public void setCacheNewRatio(int cacheNewRatio) {
        this.cacheNewRatio = cacheNewRatio;
    }

    public int getCacheMaxRatio() {
        return cacheMaxRatio;
    }

    public void setCacheMaxRatio(int cacheMaxRatio) {
        this.cacheMaxRatio = cacheMaxRatio;
    }

    public int getCacheEvictThreshold() {
        return cacheEvictThreshold;
    }

    public void setCacheEvictThreshold(int cacheEvictThreshold) {
        this.cacheEvictThreshold = cacheEvictThreshold;
    }

    public int getCacheEvictMinRatio() {
        return cacheEvictMinRatio;
    }

    public void setCacheEvictMinRatio(int cacheEvictMinRatio) {
        this.cacheEvictMinRatio = cacheEvictMinRatio;
    }

    public int getCacheFiles() {
        return cacheFiles;
    }

    public void setCacheFiles(int cacheFiles) {
        this.cacheFiles = cacheFiles;
    }

    public int getCacheMaxIdle() {
        return cacheMaxIdle;
    }

    public void setCacheMaxIdle(int cacheMaxIdle) {
        this.cacheMaxIdle = cacheMaxIdle;
    }

    public int getCacheCheckInterval() {
        return cacheCheckInterval;
    }

    public void setCacheCheckInterval(int cacheCheckInterval) {
        this.cacheCheckInterval = cacheCheckInterval;
    }

    public int getCacheEvictLatency() {
        return cacheEvictLatency;
    }

    public void setCacheEvictLatency(int cacheEvictLatency) {
        this.cacheEvictLatency = cacheEvictLatency;
    }

    public int getCacheLeakTime() {
        return cacheLeakTime;
    }

    public void setCacheLeakTime(int cacheLeakTime) {
        this.cacheLeakTime = cacheLeakTime;
    }

    /**
     * 构造器
     */
    public static class Builder {

        JournalConfig config = new JournalConfig();

        public static Builder build() {
            return new Builder();
        }

        public Builder suffix(String suffix) {
            config.setSuffix(suffix);
            return this;
        }

        public Builder dataSize(int dataSize) {
            config.setDataSize(dataSize);
            return this;
        }

        public Builder cacheSize(long cacheSize) {
            config.setCacheSize(cacheSize);
            return this;
        }

        public Builder cacheNewRatio(int cacheNewRatio) {
            config.setCacheNewRatio(cacheNewRatio);
            return this;
        }

        public Builder cacheMaxRatio(int cacheMaxRatio) {
            config.setCacheMaxRatio(cacheMaxRatio);
            return this;
        }

        public Builder cacheEvictThreshold(int cacheEvictThreshold) {
            config.setCacheEvictThreshold(cacheEvictThreshold);
            return this;
        }

        public Builder cacheEvictMinRatio(int cacheEvictMinRatio) {
            config.setCacheEvictMinRatio(cacheEvictMinRatio);
            return this;
        }

        public Builder cacheFiles(int cacheFiles) {
            config.setCacheFiles(cacheFiles);
            return this;
        }

        public Builder cacheMaxIdle(int cacheMaxIdle) {
            config.setCacheMaxIdle(cacheMaxIdle);
            return this;
        }

        public Builder cacheCheckInterval(int cacheCheckInterval) {
            config.setCacheCheckInterval(cacheCheckInterval);
            return this;
        }

        public Builder cacheEvictLatency(int cacheEvictLatency) {
            config.setCacheEvictLatency(cacheEvictLatency);
            return this;
        }

        public Builder cacheLeakTime(int cacheLeakTime) {
            config.setCacheLeakTime(cacheLeakTime);
            return this;
        }

        public JournalConfig create() {
            return config;
        }

    }
}
