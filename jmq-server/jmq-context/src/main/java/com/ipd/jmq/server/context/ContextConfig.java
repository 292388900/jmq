package com.ipd.jmq.server.context;

import com.ipd.jmq.registry.Registry;
import com.ipd.jmq.registry.RegistryFactory;

/**
 * 上下文配置
 *
 * @author lindeqiang
 * @since 2016/8/15 14:06
 */
public class ContextConfig {
    // 配置信息路径
    private String configPath;
    // 注册中心数据启用压缩模式
    protected boolean compressed;
    // 注册中心
    private Registry registry;

    private RegistryFactory registryFactory;

    private String key;

    public ContextConfig() {
    }

    public ContextConfig(String configPath, boolean compressed, Registry registry) {
        this.configPath = configPath;
        this.compressed = compressed;
        this.registry = registry;
    }

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    public boolean isCompressed() {
        return compressed;
    }

    public void setCompressed(boolean compressed) {
        this.compressed = compressed;
    }

    public Registry getRegistry() {
        return registry;
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setRegistryFactory(RegistryFactory registryFactory) {
        this.registryFactory = registryFactory;
        try{
            this.registry=registryFactory.create();
        }catch(Exception e){

        }

    }
}
