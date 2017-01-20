package com.ipd.jmq.common.network;

/**
 * 固定地址故障切换
 */
public class FixedFailoverPolicy implements FailoverPolicy {
    // 地址
    String address;

    public FixedFailoverPolicy(String address) {
        this.address = address;
    }

    @Override
    public String getAddress(String lastAddress) {
        return address;
    }
}
