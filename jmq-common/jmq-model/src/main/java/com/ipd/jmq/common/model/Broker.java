package com.ipd.jmq.common.model;

import com.ipd.jmq.common.cluster.ClusterRole;
import com.ipd.jmq.common.cluster.Permission;
import com.ipd.jmq.common.cluster.RetryType;
import com.ipd.jmq.common.cluster.SyncMode;

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
    private Identity dataCenter;
    /**
     * 主机id
     */
    private Identity host;
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
    private Identity shard;
    /**
     * 集群初始化角色
     */
    private ClusterRole role;
    /**
     * 别名
     */
    @Deprecated
    private String alias;
    /**
     * 权限
     */
    private Permission permission;
    /**
     * 复制方式
     */
    private SyncMode syncMode;
    /**
     * 重试类型
     */
    private RetryType retryType;
    /**
     * Broker实例的描述
     */
    private String description;

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

    public int getManagementPort() {
        return port % 10000 + 10000;
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

    public SyncMode getSyncMode() {
        return syncMode;
    }

    public void setSyncMode(SyncMode syncMode) {
        this.syncMode = syncMode;
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

    public Identity getDataCenter() {
        return dataCenter;
    }

    public void setDataCenter(Identity dataCenter) {
        this.dataCenter = dataCenter;
    }

    public Identity getHost() {
        return host;
    }

    public void setHost(Identity host) {
        this.host = host;
    }

    public Identity getShard() {
        return shard;
    }

    public void setShard(Identity shard) {
        this.shard = shard;
    }

    public static class Builder extends BaseModel.Builder<Broker, Builder> {
        public Builder() {
            this(new Broker());
        }

        public Builder(Broker broker) {
            super(broker);
        }

        public static Builder build() {
            return new Builder();
        }

        public static Builder build(Broker broker) {
            return new Builder(broker);
        }
    }

    public static void main(String[] args) {
        long i = Long.MAX_VALUE;
        System.out.println(Long.MAX_VALUE);
        System.out.println("i>>>32:"+(i>>>32));
        System.out.println("i^(i>>>32):"+(int)(i^(i>>>32)));
    }
}