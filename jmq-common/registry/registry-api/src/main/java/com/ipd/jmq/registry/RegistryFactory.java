package com.ipd.jmq.registry;

import com.ipd.jmq.toolkit.URL;
import com.ipd.jmq.registry.provider.AddressProvider;
import com.ipd.jmq.toolkit.plugin.PluginUtil;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 注册中心代理
 *
 * @author hexiaofeng
 * @version 1.0.0
 * @since 13-1-15 下午3:37
 */
public class RegistryFactory {
    //注册中心URL地址
    protected String url;
    //存活节点
    protected String live;
    //初始化等待时间，单位毫秒
    protected long waitTime = 5000;

    public RegistryFactory() {
    }

    public RegistryFactory(String url) {
        this(url, null, 5000);
    }

    public RegistryFactory(String url, String live) {
        this(url, live, 5000);
    }

    public RegistryFactory(String url, String live, long waitTime) {
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("address is null");
        }
        this.url = url;
        this.live = live;
        this.waitTime = waitTime;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setLive(String live) {
        this.live = live;
    }

    public void setWaitTime(long waitTime) {
        this.waitTime = waitTime;
    }

    /**
     * 创建并启动注册中心
     *
     * @return 注册中心
     * @throws Exception
     */
    public Registry create() throws Exception {
        if (url == null) {
            throw new IllegalStateException("url is null");
        }
        String address = url;
        URL ul = URL.valueOf(address);
        // 创建注册中心
        Registry registry = PluginUtil.createService(Registry.class, ul);
        if (registry == null) {
            // 判断是否是地址提供者
            AddressProvider factory = PluginUtil.createService(AddressProvider.class, ul);
            if (factory != null) {
                // 获取地址
                address = factory.getAddress();
                if (address != null && !address.isEmpty()) {
                    ul = URL.valueOf(address);
                    registry = PluginUtil.createService(Registry.class, ul);
                }
            }
        }
        if (registry == null) {
            throw new IllegalStateException("registry is null," + url);
        }
        // 启动
        registry.start();
        if (live != null && !live.isEmpty()) {
            registry.createLive(live, null);
        }
        if (waitTime > 0) {
            // 延迟等待初始化数据
            CountDownLatch latch = new CountDownLatch(1);
            try {
                latch.await(waitTime, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignored) {
            }
        }
        return registry;
    }
}
