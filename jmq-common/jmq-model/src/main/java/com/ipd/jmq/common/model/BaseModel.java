package com.ipd.jmq.common.model;

import java.io.Serializable;
import java.util.Date;

/**
 * 所有Model的基类
 *
 * @author tianya
 */
public abstract class BaseModel implements Serializable, Cloneable {

    public static final int ENABLED = Status.ENABLE.value();

    public static final int DISABLED = Status.DISABLE.value();

    public static final int DELETED = Status.DELETE.value();

    /**
     * 主键
     */
    protected long id;
    /**
     * 创建时间
     */
    protected Date createTime;
    /**
     * 创建人
     */
    protected Identity createBy;
    /**
     * 修改时间
     */
    protected Date updateTime;
    /**
     * 修改人
     */
    protected Identity updateBy;
    /**
     * 状态(默认启用)
     */
    protected int status = Status.ENABLE.value;
    /**
     * 创建人代码
     */
    protected String createUser;

    /**
     * 修改人代码
     */
    protected String updateUser;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public Identity getCreateBy() {
        return createBy;
    }

    public void setCreateBy(Identity createBy) {
        this.createBy = createBy;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getUpdateUser() {
        return updateUser;
    }

    public void setUpdateUser(String updateUser) {
        this.updateUser = updateUser;
    }

    public Identity getUpdateBy() {
        return updateBy;
    }

    public void setUpdateBy(Identity updateBy) {
        this.updateBy = updateBy;
    }

    @EnumType(Status.class)
    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BaseModel baseModel = (BaseModel) o;

        return id == baseModel.id;

    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException ignored) {
        }
        return null;
    }

    /**
     * 状态
     */
    public enum Status implements EnumItem {
        ENABLE(1, "启用"),
        DISABLE(0, "禁用"),
        DELETE(-1, "删除");

        private int value;
        private String description;

        Status(int value, String description) {
            this.value = value;
            this.description = description;
        }

        public int value() {
            return value;
        }

        public String description() {
            return description;
        }

    }

    /**
     * 构建器
     *
     * @param <M>
     * @param <T>
     */
    public static abstract class Builder<M extends BaseModel, T> {
        protected M model;

        public Builder() {
        }

        public Builder(M model) {
            this.model = model;
        }

        public T id(long id) {
            model.setId(id);
            return (T) this;
        }

        public T createTime(Date createTime) {
            model.setCreateTime(createTime);
            return (T) this;
        }

        public T createBy(Identity createBy) {
            model.setCreateBy(createBy);
            return (T) this;
        }

        public T updateTime(Date updateTime) {
            model.setUpdateTime(updateTime);
            return (T) this;
        }

        public T updateBy(Identity updateBy) {
            model.setUpdateBy(updateBy);
            return (T) this;
        }

        public T status(int status) {
            model.setStatus(status);
            return (T) this;
        }

        /**
         * 构造对象
         *
         * @return 对象
         */
        public M create() {
            return model;
        }

    }

}