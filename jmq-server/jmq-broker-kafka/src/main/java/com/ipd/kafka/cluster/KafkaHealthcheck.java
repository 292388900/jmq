package com.ipd.kafka.cluster;

import com.ipd.jmq.common.network.kafka.utils.ZKUtils;
import com.ipd.jmq.registry.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by zhangkepeng on 16-8-9.
 */
public class KafkaHealthcheck {
    private static final Logger logger = LoggerFactory.getLogger(KafkaHealthcheck.class);

    private int brokerId;
    private String host;
    private int port;
    private Registry registry;

    public KafkaHealthcheck(int brokerId, String host, int port, Registry registry) {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.registry = registry;
    }

    public void startup() throws InterruptedException{
        register();
    }

    public void shutdown() {
        try {
            ZKUtils.deregisterBrokerInZk(registry, brokerId);
        } catch (InterruptedException e) {
            logger.warn("thread interrupted exception", e);
        }
    }

    private void register() throws InterruptedException{
        String hostName = null;
        if (host == null || host.trim().isEmpty()) {
            try {
                hostName = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (UnknownHostException e) {
                logger.error("unknown host exception",e);
            }
        } else {
            hostName = host;
        }
        int jmxPort = Integer.valueOf(System.getProperty("com.sun.management.jmxremote.port", "-1"));
        ZKUtils.registerBrokerInZk(registry, brokerId, hostName, port, jmxPort);
    }
}
