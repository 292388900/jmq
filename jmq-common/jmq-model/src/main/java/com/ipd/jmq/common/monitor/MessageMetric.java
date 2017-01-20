package com.ipd.jmq.common.monitor;

import com.ipd.jmq.common.lb.TopicSession;
import com.ipd.jmq.common.model.MonitorRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wylixiaobin on 2016/12/16.
 */
public enum MessageMetric {

    APP_DISCONNECTION_TIMES("app.disconnection.times", MonitorRecord.class,"应用连接断开次数"),
    APP_CONSUME_EXPIRE_TIMES("app.consume.expire.times", MonitorRecord.class,"消费超时次数"),
    APP_PRODUCER_REMOVED_TIMES("app.producer.removed.times", MonitorRecord.class,"应用生产者移除次数"),
    APP_CONSUMER_REMOVED_TIMES("app.consumer.removed.times", MonitorRecord.class,"应用消费者移除次数"),
    BROKER_SEND_FAILURE_TIMES("broker.send.failure.times",MonitorRecord.class,"发送失败次数"),
    BROKER_SLICE_ENQUEUE("broker.slice.enqueue",MonitorRecord.class,"Broker时间段内的入队消息条数"),
    BROKER_SLICE_ENQUEUE_SIZE("broker.slice.enqueue.size",MonitorRecord.class,"Broker时间段内的入队消息大小"),
    BROKER_SLICE_DEQUEUE("broker.slice.dequeue",MonitorRecord.class,"Broker时间段内的出队消息条数"),
    BROKER_SLICE_DEQUEUE_SIZE("broker.slice.dequeue.size",MonitorRecord.class,"Broker时间段内的出队消息大小"),
    APP_SLICE_ENQUEUE("app.slice.enqueue",MonitorRecord.class,"应用时间段内的入队消息条数"),
    APP_SLICE_ENQUEUE_SIZE("app.slice.enqueue.size",MonitorRecord.class,"应用时间段内的入队消息大小"),


    BROKER_STORAGE("broker.storage",MonitorRecord.class,"存储占用"),
    BROKER_STORAGE_USAGE("broker.storage.usage",MonitorRecord.class,"存储占用百分比(报警)"),
    BROKER_WRITE_SPEED("broker.storage.speed",MonitorRecord.class,"存储写入速度"),
    BROKER_CONNECTION("broker.mmap.usage",MonitorRecord.class,"连接数(报警)"),
    BROKER_MMAP_USAGE("",MonitorRecord.class,"内存镜像文件占用百分比(报警)"),
    BROKER_MMAP("broker.mmap",MonitorRecord.class,"内存镜像文件占用内存空间"),
    BROKER_ENQUEUE("broker.enqueue",MonitorRecord.class,"Broker入队累计条数"),
    BROKER_ENQUEUE_SIZE("broker.enqueue.size",MonitorRecord.class,"Broker入队累计大小"),
    BROKER_DEQUEUE("broker.dequeue",MonitorRecord.class,"broker出队累计条数"),
    BROKER_DEQUEUE_SIZE("broker.dequeue.size",MonitorRecord.class,"Broker出队累计大小"),
    BROKER_PENDING("broker.pending",MonitorRecord.class,"Broker当前积压数量"),
    BROKER_MAXJOURNALOFFSET("broker.max.journal.offset",MonitorRecord.class,"Broker当前最大日志位置"),
    BROKER_JOURNAL_BADFILE("broker.journal.badfile",MonitorRecord.class,"从节点是否有坏块"),
    APP_CONNECTION("app.connection",MonitorRecord.class,"应用连接数(报警)"),
    APP_PRODUCE("app.produce",MonitorRecord.class,"应用生产者数量(报警)"),
    APP_CONSUME("app.consume",MonitorRecord.class,"应用消费者数量(报警)"),
    APP_ENQUEUE("app.enqueue",MonitorRecord.class,"应用入队累计"),
    APP_DEQUEUE("app.dequeue",MonitorRecord.class,"应用出队累计"),
    APP_PENDING("app.pending",MonitorRecord.class,"应用消费积压(报警)"),

    TOPIC_SESSION("topic.session",TopicSession.class,"主题应用实例");






    private static Map<String, MessageMetric> instructionsMap = new HashMap<String, MessageMetric>();

    private String identityType;

    private Class cls;

    private String desc;

    public String getIdentityType() {
        return identityType;
    }

    public Class getCls() {
        return cls;
    }

    public java.lang.String getDesc() {
        return desc;
    }

    MessageMetric(String identityType, Class cls, String desc) {
        this.identityType = identityType;
        this.cls = cls;
        this.desc = desc;
    }

    public static MessageMetric findIdentityType(String instruction) {
        if (instructionsMap.isEmpty()) {
            synchronized (instructionsMap) {
                if (instructionsMap.isEmpty()) {
                    for (MessageMetric messageMetric : MessageMetric.values()) {
                        instructionsMap.put(messageMetric.getIdentityType(), messageMetric);
                    }
                }
            }
        }
        return instructionsMap.get(instruction);
    }
}
