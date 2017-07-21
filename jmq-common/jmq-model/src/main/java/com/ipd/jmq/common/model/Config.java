package com.ipd.jmq.common.model;


/**
 * 配置管理对象
 */
public class Config extends BaseModel {
    // 分组
    private String type;
    // 键
    private String code;
    // 名称
    private String name;
    // 值
    private String value;
    // 密码保护
    private boolean password = false;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        if (name == null && code != null) {
            return code;
        }
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public boolean isPassword() {
        return password;
    }

    public void setPassword(boolean password) {
        this.password = password;
    }
}
