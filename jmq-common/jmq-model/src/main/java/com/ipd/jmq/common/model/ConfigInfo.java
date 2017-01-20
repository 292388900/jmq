package com.ipd.jmq.common.model;



/**
 * 配置管理对象
 */
public class ConfigInfo extends BaseModel {
    //通用分组
    public static String GROUP_BROKER = "broker";
	// 分组
	private String group;
	// 键
	private String key;
	// 值
	private String value;

	private boolean password = false;

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
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
