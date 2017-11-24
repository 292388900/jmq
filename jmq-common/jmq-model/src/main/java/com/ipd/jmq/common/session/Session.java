package com.ipd.jmq.common.session;

/**
 * 会话接口
 * Created by hexiaofeng on 15-3-2.
 */
public interface Session {
    /**
     * 用户KEY
     */
    String USER = "user";

    /**
     * 获取当前用户信息
     *
     * @return 当前用户信息
     */
    UserDetail getUser();

    /**
     * 获取用户ID
     *
     * @return 用户ID
     */
    long getUserId();

    /**
     * 设置当前会话用户信息
     *
     * @param user 用户信息
     */
    void setUser(UserDetail user);
}
