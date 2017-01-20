package com.ipd.jmq.registry.provider;


import com.ipd.jmq.toolkit.plugin.ServicePlugin;

/**
 * 注册中心工厂类
 *
 * @author hexiaofeng
 * @version 1.0.0
 * @since 12-12-12 下午4:09
 */
public interface AddressProvider extends ServicePlugin {

    /**
     * 获取地址
     *
     * @return 地址
     */
    String getAddress() throws Exception;

}
