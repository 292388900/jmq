package com.ipd.jmq.common.network;

import java.util.List;

/**
 * 轮询故障切换策略
 */
public class RoundRobinFailoverPolicy implements FailoverPolicy {
    // 地址
    protected List<String> addresses;

    public RoundRobinFailoverPolicy() {
    }

    public RoundRobinFailoverPolicy(List<String> addresses) {
        this.addresses = addresses;
    }

    public List<String> getAddresses() {
        return addresses;
    }

    public void setAddresses(List<String> addresses) {
        this.addresses = addresses;
    }

    @Override
    public String getAddress(final String lastAddress) {
        if (addresses == null || addresses.isEmpty()) {
            return null;
        }
        int pos = 0;
        if (lastAddress != null) {
            for (int i = 0; i < addresses.size(); i++) {
                if (lastAddress.equals(addresses.get(i))) {
                    pos = i + 1;
                    break;
                }
            }
        }
        if (pos >= addresses.size()) {
            pos = 0;
        }
        return addresses.get(pos);
    }
}
