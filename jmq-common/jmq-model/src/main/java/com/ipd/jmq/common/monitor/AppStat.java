package com.ipd.jmq.common.monitor;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * app统计信息
 *
 * @author xuzhenhua
 */
public class AppStat implements Serializable {

    private static final long serialVersionUID = -1925788827902461246L;
    // 应用代码
    private String app;
    // 是否是生产者
    private boolean producerRole = false;
    // 是否是消费者
    private boolean consumerRole = false;
    // 总连接数
    private AtomicLong connection = new AtomicLong(0);
    // 生产者数量
    private AtomicLong producer = new AtomicLong(0);
    // 消费者数量
    private AtomicLong consumer = new AtomicLong(0);
    // 服务端入队消息数量
    private AtomicLong enQueue = new AtomicLong(0);
    // 服务端入队消息大小(单位字节)
    private AtomicLong enQueueSize = new AtomicLong(0);
    // 服务端出队消息数量
    private AtomicLong deQueue = new AtomicLong(0);
    // 服务端出队消息大小(单位字节)
    private AtomicLong deQueueSize = new AtomicLong(0);
    // 积压消息数量
    private AtomicLong pending = new AtomicLong(0);
    // 重试数量
    private AtomicLong retry = new AtomicLong(0);
    // 连接
    private transient ConcurrentMap<String, Client> clients = new ConcurrentHashMap<String, Client>();

    public AppStat() {
    }

    public AppStat(String app) {
        this.app = app;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public boolean isProducerRole() {
        return producerRole;
    }

    public void setProducerRole(boolean producerRole) {
        this.producerRole = producerRole;
    }

    public boolean isConsumerRole() {
        return consumerRole;
    }

    public void setConsumerRole(boolean consumerRole) {
        this.consumerRole = consumerRole;
    }

    public AtomicLong getConnection() {
        return connection;
    }

    public void setConnection(AtomicLong connection) {
        this.connection = connection;
    }

    public AtomicLong getProducer() {
        return producer;
    }

    public void setProducer(AtomicLong producer) {
        this.producer = producer;
    }

    public AtomicLong getConsumer() {
        return consumer;
    }

    public void setConsumer(AtomicLong consumer) {
        this.consumer = consumer;
    }

    public AtomicLong getEnQueue() {
        return enQueue;
    }

    public void setEnQueue(AtomicLong enQueue) {
        this.enQueue = enQueue;
    }

    public AtomicLong getEnQueueSize() {
        return enQueueSize;
    }

    public void setEnQueueSize(AtomicLong enQueueSize) {
        this.enQueueSize = enQueueSize;
    }

    public AtomicLong getDeQueue() {
        return deQueue;
    }

    public void setDeQueue(AtomicLong deQueue) {
        this.deQueue = deQueue;
    }

    public AtomicLong getDeQueueSize() {
        return deQueueSize;
    }

    public void setDeQueueSize(AtomicLong deQueueSize) {
        this.deQueueSize = deQueueSize;
    }

    public AtomicLong getPending() {
        return pending;
    }

    public void setPending(AtomicLong pending) {
        this.pending = pending;
    }

    public AtomicLong getRetry() {
        return retry;
    }

    public void setRetry(AtomicLong retry) {
        this.retry = retry;
    }

    public ConcurrentMap<String, Client> getClients() {
        return clients;
    }

    public void setClients(ConcurrentMap<String, Client> clients) {
        this.clients = clients;
    }

}
