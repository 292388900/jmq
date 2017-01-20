package com.ipd.jmq.server.store;

/**
 * 所有者
 */
public class OwnerShip {
    // 所有者
    private String owner;
    // 占用过期事件
    private long expireTime;

    private long createTime;

    public OwnerShip(String owner, long expireTime) {
        this.owner = owner;
        this.expireTime = expireTime;
    }

    public String getOwner() {
        return owner;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public boolean isExpire(long now) {
        return expireTime > 0 && expireTime <= now;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OwnerShip{");
        sb.append("owner='").append(owner).append('\'');
        sb.append(", create=").append(createTime);
        sb.append(", expireTime=").append(expireTime);
        sb.append(", times:").append(expireTime - createTime);
        sb.append('}');
        return sb.toString();
    }
}
