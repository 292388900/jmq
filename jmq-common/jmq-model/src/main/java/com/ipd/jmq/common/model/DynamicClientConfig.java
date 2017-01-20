package com.ipd.jmq.common.model;

import java.util.HashMap;
import java.util.Map;

/**
 * DynamicClientConfig
 *
 * Dynamic config for client
 *
 * @author luoruiheng
 * @since 11/30/16
 */
public class DynamicClientConfig {

    // app name
    private String app;

    // producer config
    private Map<String, ProducerConfig> producerConfigMap = new HashMap<String, ProducerConfig>();

    // consumer config
    private Map<String, ConsumerConfig> consumerConfigMap = new HashMap<String, ConsumerConfig>();

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public Map<String, ProducerConfig> getProducerConfigMap() {
        return producerConfigMap;
    }

    public void setProducerConfigMap(Map<String, ProducerConfig> producerConfigMap) {
        this.producerConfigMap = producerConfigMap;
    }

    public Map<String, ConsumerConfig> getConsumerConfigMap() {
        return consumerConfigMap;
    }

    public void setConsumerConfigMap(Map<String, ConsumerConfig> consumerConfigMap) {
        this.consumerConfigMap = consumerConfigMap;
    }

}
