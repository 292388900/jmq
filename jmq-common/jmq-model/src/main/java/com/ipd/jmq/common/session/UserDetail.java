package com.ipd.jmq.common.session;

import com.ipd.jmq.common.model.Identity;

/**
 * 用户信息
 * Created by hexiaofeng on 15-8-26.
 */
public class UserDetail extends Identity {

    // 角色
    protected Identity[] roles;

    public UserDetail() {
    }

    public UserDetail(long id, String code, String name, Identity... roles) {
        this.id = id;
        this.code = code;
        this.name = name;
        this.roles = roles;
    }

    public Identity[] getRoles() {
        return roles;
    }

    public void setRoles(Identity[] roles) {
        this.roles = roles;
    }

}
