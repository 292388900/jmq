package com.ipd.jmq.toolkit.security.auth;


import java.util.Map;

/**
 * 默认认证
 */
public class DefaultAuthentication implements Authentication {
    // 管理员用户
    private String adminUser;
    // 管理员密码
    private String adminPassword;
    // 用户
    private Map<String, String> users;
    private String tokenPrefix = "";

    public DefaultAuthentication() {
    }

    public DefaultAuthentication(String adminUser, String adminPassword) {
        this.adminUser = adminUser;
        this.adminPassword = adminPassword;
    }

    public DefaultAuthentication(String adminUser, String adminPassword, String prefix) {
        this.adminUser = adminUser;
        this.adminPassword = adminPassword;
        this.tokenPrefix = prefix;
    }

    public String getAdminUser() {
        return adminUser;
    }

    public void setAdminUser(String adminUser) {
        this.adminUser = adminUser;
    }

    public String getAdminPassword() {
        return adminPassword;
    }

    public void setAdminPassword(String adminPassword) {
        this.adminPassword = adminPassword;
    }

    public Map<String, String> getUsers() {
        return users;
    }

    public void setUsers(Map<String, String> users) {
        this.users = users;
    }

    /**
     * 根据用户名产生密码
     *
     * @param user
     * @return
     */
    public static String createPassword(final String user) throws AuthException {
           return user;
    }

    @Override
    public UserDetails getUser(final String user) throws AuthException {
        boolean admin = false;
        String password;
        if (adminUser != null && adminUser.equalsIgnoreCase(user)) {
            password = adminPassword;
            admin = true;
        } else if (users != null && users.containsKey(user)) {
            // 兼容原有密码
            password = users.get(user);
        } else {
            // 原始密码
            password = createPassword(tokenPrefix + user);
        }


        return new DefaultUserDetail(user, password, admin);
    }

    @Override
    public PasswordEncoder getPasswordEncode() {
        return null;
    }

}