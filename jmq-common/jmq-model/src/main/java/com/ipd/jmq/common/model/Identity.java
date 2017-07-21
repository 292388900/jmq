package com.ipd.jmq.common.model;

import java.io.Serializable;

/**
 * 用于关联信息，便于显示
 * Created by hexiaofeng on 16-8-9.
 */
public class Identity implements Serializable {
    // ID
    protected long id;
    // 编号
    protected String code;
    // 名称
    protected String name;
    // 状态
    protected int status;

    public Identity() {
    }

    public Identity(long id) {
        this.id = id;
    }

    public Identity(long id, String code, String name, int status) {
        this.id = id;
        this.code = code;
        this.name = name;
        this.status = status;
    }

    public Identity(String code) {
        this.code = code;
    }

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

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Identity identity = (Identity) o;

        if (id != identity.id) {
            return false;
        }
        return code != null ? code.equals(identity.code) : identity.code == null;

    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (code != null ? code.hashCode() : 0);
        return result;
    }
}
