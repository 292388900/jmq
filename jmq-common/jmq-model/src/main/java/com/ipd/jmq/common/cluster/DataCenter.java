package com.ipd.jmq.common.cluster;

import com.ipd.jmq.common.model.Subnet;

import java.io.Serializable;
import java.util.List;

/**
 * Created by lining on 17-3-7.
 */
public class DataCenter implements Serializable{
    //id
    private long id;
    // 代码
    private String code;
    // 名称
    private String name;
    // 是否支持弹性伸缩
    private boolean scalable;
    // 描述
    private String description;
    //子网
    private List<Subnet> subnets;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

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

    public List<Subnet> getSubnets() {
        return subnets;
    }

    public void setSubnets(List<Subnet> subnets) {
        this.subnets = subnets;
    }
}
