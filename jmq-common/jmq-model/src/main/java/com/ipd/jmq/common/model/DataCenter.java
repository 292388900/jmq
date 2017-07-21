package com.ipd.jmq.common.model;

/**
 * 数据中心
 */
public class DataCenter extends BaseModel {
    // 代码
    private String code;
    // 名称
    private String name;
    // 是否支持弹性伸缩
    private boolean scalable;
    // 描述
    private String description;

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isScalable() {
        return scalable;
    }

    public void setScalable(boolean scalable) {
        this.scalable = scalable;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
