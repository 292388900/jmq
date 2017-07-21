package com.ipd.jmq.demo.producer;

import com.ipd.jmq.client.transaction.TxStatusQuerier;
import com.ipd.jmq.common.network.v3.command.TxStatus;
import org.apache.log4j.Logger;


/**
 * LocalTxStatusQuerier
 *
 * 事务状态的查询器
 * 需要实现 TxStatusQuerier 接口以便 TxFeedbackManager 反查
 *
 * @author luoruiheng
 * @date 6/8/16
 */
public class LocalTxStatusQuerier implements TxStatusQuerier {

    private static final Logger logger = Logger.getLogger(LocalTxStatusQuerier.class);
    private String localId = null;



    @Override
    public TxStatus queryStatus(String txId, String queryId) {

        if (txId == null || txId.isEmpty()) {
            return TxStatus.UNKNOWN;
        }
        int value = Integer.parseInt(TxStatusUtil.get(txId, TxStatus.UNKNOWN.ordinal() + ""));

        if (!txId.equals(localId)) {
            logger.info("feedback manager is querying txId : " + txId);
            logger.info("its value is : " + TxStatus.valueOf(value));
            localId = txId;
        }

        return TxStatus.valueOf(value);
    }

}
