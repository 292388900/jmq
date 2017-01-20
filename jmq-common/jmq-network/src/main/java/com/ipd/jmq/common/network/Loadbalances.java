package com.ipd.jmq.common.network;

import java.util.List;

/**
 * 负载均衡算法工具类.
 */
public class Loadbalances {

    /**
     * 随机权重算法
     *
     * @param transports 通道
     * @return 选择的通道
     */
    public static final <T extends FailoverTransport> T randomWeight(final List<T> transports) {
        // 根据连接状态计算权重
        if (transports == null) {
            return null;
        }
        int size = transports.size();
        if (size == 0) {
            return null;
        } else if (size == 1) {
            return transports.get(0);
        }
        int[] weights = new int[size];
        int seed = 0;
        int weight = 0;
        int index = 0;
        // 遍历所有通道计算有效连接的权重
        for (FailoverTransport transport : transports) {
            weight = transport.getState() == FailoverState.CONNECTED ? transport.getWeight() : 0;
            weight = weight < 0 ? 0 : weight;
            weights[index] = weight;
            seed += weight;
            index++;
        }
        if (seed > 0) {
            // 计算随机权重，返回在[1,seed]之间
            int random = (int) (Math.random() * seed) + 1;
            seed = 0;
            // 根据权重找到连接
            for (int i = 0; i < weights.length; i++) {
                seed += weights[i];
                if (random <= seed) {
                    return transports.get(i);
                }
            }
        }
        // 没有一个可用的通道，则随机选择一个
        return transports.get((int) (Math.random() * size));
    }

}
