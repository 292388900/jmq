package com.ipd.jmq.registry.provider;

import com.ipd.jmq.toolkit.URL;
import com.ipd.jmq.toolkit.plugin.PluginUtil;

/**
 * Failover注册中心
 *
 * @author hexiaofeng
 * @version 1.0.0
 * @since 12-12-12 下午5:03
 */
public class FailoverProvider implements AddressProvider {

    protected URL url;

    @Override
    public String getType() {
        return "failover";
    }

    @Override
    public void setUrl(URL url) {
        this.url = url;
    }

    @Override
    public String getAddress() throws Exception {
        if (url == null) {
            throw new IllegalStateException("url is null");
        }
        if (url.getHost() == null) {
            throw new IllegalStateException("url is invalid");
        }
        String address;
        Exception error = null;
        AddressProvider provider;
        String[] urls = URL.split(url.getHost());
        for (String value : urls) {
            try {
                // 把父参数带入进去
                provider = PluginUtil
                        .createService(AddressProvider.class, URL.valueOf(value).addIfAbsent(url.getParameters()));
                if (provider != null) {
                    address = provider.getAddress();
                    if (address != null && !address.isEmpty()) {
                        return address;
                    }
                }
            } catch (Exception e) {
                error = e;
            }
        }
        if (error != null) {
            throw error;
        }
        return null;
    }
}
