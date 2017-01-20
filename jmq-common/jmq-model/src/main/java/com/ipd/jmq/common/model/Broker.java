package com.ipd.jmq.common.model;

import com.ipd.jmq.common.cluster.ClusterRole;
import com.ipd.jmq.common.cluster.Permission;
import com.ipd.jmq.common.cluster.RetryType;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.io.Serializable;
import java.util.StringTokenizer;

/**
 * Broker实例
 *
 * @author tianya
 */
public class Broker extends BaseModel implements Serializable {
    public static final int READ_ONLY = 2;

    public static final String JMQ2 = "2.0";
    public static final String OCT = "3.0";
    /**
     * 数据中心Id
     */
    private long dataCenterId;
    /**
     * Broker实例的名称
     */
    private String name;
    /**
     * Broker实例的ip
     */
    @NotNull(message = "The ip can not be null")
    @Pattern(regexp = "^[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}$", message = "Please enter correct ip")
    private String ip;
    /**
     * Broker实例的端口号
     */
    @NotNull(message = "The port can not be null")
    @Min(value = 100, message = "Please enter 100 to 65535")
    @Max(value = 65535, message = "Please enter 100 to 65535")
    private int port = 50088;
    /**
     * 分组
     */
    private String group;
    /**
     * 集群初始化角色
     */
    private ClusterRole role;
    /**
     * 别名
     */
    private String alias;
    /**
     * 权限
     */
    private Permission permission;
    /**
     * 重试类型
     */
    private RetryType retryType;
    /**
     * 重试类型
     */
    private String flushDiskMode;
    /**
     * Broker实例的描述
     */
    private String description;

    private String labels;

    public String getLabels() {
        return labels;
    }

    public void setLabels(String labels) {
        this.labels = labels;
    }

    private String version;

    public Broker() {
        super();
    }

    public Broker(String name) {
        if (name != null && !name.isEmpty()) {
            String[] parts = new String[]{null, null, null, null, null};
            int index = 0;
            StringTokenizer tokenizer = new StringTokenizer(name, "_");
            while (tokenizer.hasMoreTokens()) {
                parts[index++] = tokenizer.nextToken();
                if (index >= parts.length) {
                    break;
                }
            }
            if (index < 5) {
                throw new IllegalArgumentException("broker name " + name + " is invalid");
            }
            this.ip = parts[0] + "." + parts[1] + "." + parts[2] + "." + parts[3];
            this.port = Integer.valueOf(parts[4]);
        }
    }

    public Broker(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public Broker(String ip, int port, String alias) {
        this.ip = ip;
        this.port = port;
        this.alias = alias;
    }

    public long getDataCenterId() {
        return dataCenterId;
    }

    public void setDataCenterId(long dataCenterId) {
        this.dataCenterId = dataCenterId;
    }

    public String getName() {
        if (name == null && ip != null) {
            name = ip.replace('.', '_') + "_" + port;
        }
        return name;
    }

    public int getManagementPort() {
        return port % 10000 + 10000;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip.trim();
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public ClusterRole getRole() {
        return role;
    }

    public void setRole(ClusterRole role) {
        this.role = role;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public Permission getPermission() {
        return permission;
    }

    public void setPermission(Permission permission) {
        this.permission = permission;
    }

    public RetryType getRetryType() {
        return retryType;
    }

    public void setRetryType(RetryType retryType) {
        this.retryType = retryType;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getFlushDiskMode() {
        return flushDiskMode;
    }

    public void setFlushDiskMode(String flushDiskMode) {
        this.flushDiskMode = flushDiskMode;
    }

    @Override
    public String toString() {
        return this.getName();
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}