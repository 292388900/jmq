package com.ipd.jmq.replication;

import com.ipd.jmq.toolkit.stat.TPStatBuffer;
import com.ipd.jmq.toolkit.stat.TPStatDoubleBuffer;
import com.ipd.jmq.toolkit.stat.TPStatSlice;
import com.ipd.jmq.toolkit.time.NanoPeriod;
import com.ipd.jmq.toolkit.time.Period;

import java.math.BigDecimal;

/**
 * 复制性能统计.
 *
 * @author lindeqiang
 * @since 2016/9/6 16:06
 */
public class RepStat extends TPStatDoubleBuffer<RepStat.RepStatSlice> {
    public static final int ONE_MINUTE = 1000 * 60;

    public RepStat() {
        super(new RepStatSlice(), new RepStatSlice(), ONE_MINUTE);
    }

    /**
     * 成功写入
     *
     * @param count 记录调试
     * @param size  大小
     * @param time  时间
     */
    public void success(final int count, final long size, final int time) {
            lock.readLock().lock();
            try {
                writeStat.getRepStat().success(count, size, time);
            } finally {
                lock.readLock().unlock();
            }
    }

    /**
     * 错误
     */
    public void error() {
            lock.readLock().lock();
            try {
                writeStat.getRepStat().error();
            } finally {
                lock.readLock().unlock();
            }
    }


    public static class RepStatSlice implements TPStatSlice {
        NanoPeriod period = new NanoPeriod();

        TPStatBuffer repStat = new TPStatBuffer();

        @Override
        public Period getPeriod() {
            return period;
        }

        public TPStatBuffer getRepStat() {
            return repStat;
        }

        public double getRepSpeed() {
            long size = repStat.getTPStat().getSize();
            long time = repStat.getTPStat().getTime();
            double diskSpeed = 0.0;
            if (time > 0) {
                BigDecimal bg = new BigDecimal(size * 1000000000.0 / time);
                diskSpeed = bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
            }
            return diskSpeed;
        }

        @Override
        public void clear() {
            repStat.clear();
            period.clear();
        }
    }
}
