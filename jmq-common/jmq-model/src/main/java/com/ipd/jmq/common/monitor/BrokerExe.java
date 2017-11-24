package com.ipd.jmq.common.monitor;

import com.ipd.jmq.toolkit.stat.TPStatSlice;
import com.ipd.jmq.toolkit.time.MilliPeriod;
import com.ipd.jmq.toolkit.time.Period;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * broker异常
 *
 * @author xuzhenhua
 */
public class BrokerExe implements TPStatSlice, Serializable {
    private static final long serialVersionUID = 3704812359800945167L;
    // broker名称
    private String name;
    // broker分组
    private String group;
    // 应用异常信息
    protected ConcurrentMap<String, AppExceptionStat> exceptionStats = new ConcurrentHashMap<String,
            AppExceptionStat>();
    //发送失败次数
    protected AtomicLong sendFailureTimes = new AtomicLong(0);
    // 时间片
    MilliPeriod period = new MilliPeriod();

    public BrokerExe(String name) {
        this.name = name;
    }

    public BrokerExe(String name, String group) {
        this.name = name;
        this.group = group;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ConcurrentMap<String, AppExceptionStat> getExceptionStats() {
        return exceptionStats;
    }

    public void setExceptionStats(ConcurrentMap<String, AppExceptionStat> exceptionStats) {
        this.exceptionStats = exceptionStats;
    }

    public AtomicLong getSendFailureTimes() {
        return sendFailureTimes;
    }

    public void setSendFailureTimes(AtomicLong sendFailureTimes) {
        this.sendFailureTimes = sendFailureTimes;
    }

    public AppExceptionStat getAndCreateAppExceptionStat(String app) {
        if (app == null) {
            return null;
        }

        AppExceptionStat exceptionStat = exceptionStats.get(app);
        if (exceptionStat == null) {
            exceptionStat = new AppExceptionStat(app);
            AppExceptionStat old = exceptionStats.putIfAbsent(app, exceptionStat);
            if (old != null) {
                exceptionStat = old;
            }

        }
        return exceptionStat;
    }

    @Override
    public Period getPeriod() {
        return period;
    }

    @Override
    public void clear() {
        sendFailureTimes.set(0);
        for (AppExceptionStat exceptionStat : exceptionStats.values()) {
            exceptionStat.clear();
        }
    }
}
