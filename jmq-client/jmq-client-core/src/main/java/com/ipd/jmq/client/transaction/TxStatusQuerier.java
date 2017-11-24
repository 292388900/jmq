package com.ipd.jmq.client.transaction;


import com.ipd.jmq.common.network.v3.command.TxStatus;

/**
 * 事务状态查询器.
 *
 * @author lindeqiang
 * @since 2016/5/26 17:42
 */
public interface TxStatusQuerier {
    /**
     * 查询事务状态
     *
     * @param txId    事务ID
     * @param queryId 查询ID
     * @return 事务状态
     */
    TxStatus queryStatus(String txId, String queryId);
}
