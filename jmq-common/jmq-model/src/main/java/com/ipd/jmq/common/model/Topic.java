package com.ipd.jmq.common.model;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

/**
 * 主题
 */
public class Topic extends BaseModel {
    public static final int DEFAULT_QUEUE_SIZE = 5;
    public static final int DEFAULT_SEQ_QUEUE_SIZE =1;
    public static final int TOPIC_TYPE_TOPIC = 0;
    public static final int TOPIC_TYPE_BROADCAST = 1;
    public static final int TOPIC_TYPE_SEQUENTIAL = 2;
    public static final int NEW = 2;

    /**
     * 消息类型名称
     */
    @NotNull
    private String name;
    /**
     * 消息类型代码
     */
    @Pattern(regexp = "^[a-zA-Z0-9]+[a-zA-Z0-9_]*[a-zA-Z0-9]+$", message = "Please enter correct code")
    private String code;
    /**
     * 队列数
     */
    @Min(0)
    @Max(99)
    private int queues = DEFAULT_QUEUE_SIZE;
    /**
     * 是否归档
     */
    private boolean archive = false;
    /**
     * 消息类型描述
     */
    private String description;
    /**
     * 类型 0:topic,1:broadcast,2:sequential
     */
    private int type;

    private String labels;

    public String getLabels() {
        return labels;
    }

    public void setLabels(String labels) {
        this.labels = labels;
    }

    public Topic() {
    }

    public Topic(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public int getQueues() {
        return queues;
    }

    public void setQueues(int queues) {
        this.queues = queues;
    }

    public boolean isArchive() {
        return archive;
    }

    public void setArchive(boolean archive) {
        this.archive = archive;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
}