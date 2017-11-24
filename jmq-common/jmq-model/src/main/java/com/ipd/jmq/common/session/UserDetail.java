package com.ipd.jmq.common.session;

import com.ipd.jmq.common.model.Identity;

/**
 * 用户信息
 * Created by hexiaofeng on 15-8-26.
 */
public class UserDetail extends Identity {

    // 角色
    protected Identity[] roles;

    protected boolean isAdmin;

    public UserDetail() {
    }

    public UserDetail(long id, String code, String name, boolean isAdmin) {
        this.id = id;
        this.code = code;
        this.name = name;
        this.isAdmin = isAdmin;
    }

    public Identity[] getRoles() {
        return roles;
    }

    public void setRoles(Identity[] roles) {
        this.roles = roles;
    }

    public boolean isAdmin() {
        return isAdmin;
    }

    public void setIsAdmin(boolean isAdmin) {
        this.isAdmin = isAdmin;
    }
}
