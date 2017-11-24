package com.ipd.jmq.common.model;

/**
 * 用户类型
 */
public enum UserType implements EnumItem{
    /**
     * 管理员
     */
    ADMIN(1, "admin"),
    /**
     * 业务系统用户
     */
    USER(2, "user"),
    /**
     * 系统监控
     */
    MONITOR(3,"monitor"),
    /**
     * 管理员和系统用户
     */
    ALL(4,"all");

    private int value;
    private String description;

    UserType(int value, String description) {
        this.value = value;
        this.description = description;
    }

    @Override
    public int value() {
        return value;
    }

    @Override
    public String description(){
        return description;
    }

    public static UserType valueOf(int code){
        UserType result=null;
        UserType[] userTypes=  UserType.values();
        if(null!=userTypes){
            for(UserType userType:userTypes){
                if(userType.value() == code){
                    result=userType;
                }
            }
        }
        return  result;
    }

}
