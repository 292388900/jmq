package com.ipd.jmq.client.connection;

import com.ipd.jmq.common.network.v3.session.ProducerId;

/**
 * 生产者连接接口
 */
public interface ProducerTransport extends GroupTransport {

    /**
     * 返回生产者ID
     *
     * @return 生产者ID
     */
    ProducerId getProducerId();

}
