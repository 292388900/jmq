package com.ipd.jmq.common.network;

/**
 * 故障切换策略
 */
public interface FailoverPolicy {

    /**
     * 获取目标策略
     *
     * @param lastAddress 上一次地址
     * @return 新的地址
     */
    String getAddress(String lastAddress);

}
