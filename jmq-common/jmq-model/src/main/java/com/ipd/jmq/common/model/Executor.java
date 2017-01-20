/*
 * 
 */
package com.ipd.jmq.common.model;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 * 执行器
 */
public class Executor extends BaseModel {
    // IP
    private String ip;
    // JMX端口
    @Min(1025)
    @Max(65535)
    private int jmxPort;
    // 执行器名称
    private String name;
    // 执行器类型
    private ExecutorType type;
    // 描述
    private String description;

    public Executor() {
    }

    public Executor(ExecutorType type) {
        this.type = type;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getJmxPort() {
        return jmxPort;
    }

    public void setJmxPort(int jmxPort) {
        this.jmxPort = jmxPort;
    }

    public String getName() {
        if (name == null) {
            if (ip != null && jmxPort > 0) {
                name = ip + "_" + jmxPort;
            }
        }
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ExecutorType getType() {
        return type;
    }

    public void setType(ExecutorType type) {
        this.type = type;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return getName();
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    /**
     * 执行器类型
     */
    public static enum ExecutorType {
        /**
         * 任务
         */
        TASK,
        /**
         * 报警
         */
        ALARM,
        /**
         * 归档
         */
        ARCHIVE,
        /**
         * 代理
         */
        AGENT
    }

}
