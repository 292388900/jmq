package com.ipd.jmq.common.network.v3.command;

/**
 * 健康检查，要判断读写权限和生产者是否存在
 */
public class GetProducerHealth extends GetHealth {
    // 生产者ID
    private String producerId;

    @Override
    public int predictionSize() {
        return super.predictionSize() + Serializer.getPredictionSize(producerId);
    }

    public GetProducerHealth topic(String topic) {
        setTopic(topic);
        return this;
    }

    public GetProducerHealth app(String app) {
        setApp(app);
        return this;
    }

    public GetProducerHealth producerId(String producerId) {
        setProducerId(producerId);
        return this;
    }

    public GetProducerHealth dataCenter(byte dataCenter) {
        setDataCenter(dataCenter);
        return this;
    }

    public String getProducerId() {
        return producerId;
    }

    public void setProducerId(String producerId) {
        this.producerId = producerId;
    }

    @Override
    public int type() {
        return CmdTypes.GET_PRODUCER_HEALTH;
    }
}