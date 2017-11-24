package com.ipd.jmq.common.session;


/**
 * 本地线程会话
 * Created by hexiaofeng on 15-3-3.
 */
public class LocalSession implements Session {

    private static final ThreadLocal<UserDetail> local = new ThreadLocal<UserDetail>();

    private static final Session session = new LocalSession();

    @Override
    public UserDetail getUser() {
        return local.get();
    }

    @Override
    public long getUserId() {
        UserDetail user = local.get();
        return user == null ? 0 : user.getId();
    }

    @Override
    public void setUser(UserDetail user) {
        local.set(user);
    }

    /**
     * 单例函数
     *
     * @return 会话
     */
    public static Session getSession() {
        return session;
    }
}
