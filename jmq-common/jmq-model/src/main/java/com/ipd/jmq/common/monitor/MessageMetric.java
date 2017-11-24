package com.ipd.jmq.common.monitor;

import com.ipd.jmq.common.lb.TopicSession;
import com.ipd.jmq.common.model.MonitorRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wylixiaobin on 2016/12/16.
 */
public enum MessageMetric {

    /**
     * Broker维度的，以Broker开头
     * Broker-Topic维度的，以Topic开头
     * Broker-Topic-App维度的，以App开头
     *
     * 需要汇总的数据，在源指标后面加Sum
     *
     * 主机相关的以Machine开头
     *
     * EnQueue为入队条数
     * DeQueue为出队条数
     * EnQueueSize为入队流量单位为B
     * DeQueueSize为出队流量单位为B
     *
     * 时间维度分为：十秒,Second;分钟,Minute;小时,Hour;天,Day;
     */

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
    BROKER_CONNECTION("broker.connection",MonitorRecord.class,"连接数(报警)"),
    BROKER_MMAP_USAGE("",MonitorRecord.class,"内存镜像文件占用百分比(报警)"),
    BROKER_MMAP("broker.mmap",MonitorRecord.class,"内存镜像文件占用内存空间"),

//    BROKER_ENQUEUE("broker.enqueue",MonitorRecord.class,"Broker入队累计条数"),
//    BROKER_ENQUEUE_SIZE("broker.enqueue.size",MonitorRecord.class,"Broker入队累计大小"),

    BROKER_ENQUEUE_SECOND("broker.enqueue.second",MonitorRecord.class,"Broker每10秒入队条数"),
    BROKER_ENQUEUE_SIZE_SECOND("broker.enqueue.size.second",MonitorRecord.class,"Broker每十秒入队大小"),

    BROKER_ENQUEUE_MINUTE("broker.enqueue.minute",MonitorRecord.class,"Broker每10秒入队条数"),
    BROKER_ENQUEUE_SIZE_MINUTE("broker.enqueue.size.minute",MonitorRecord.class,"Broker每十秒入队大小"),

    BROKER_ENQUEUE_HOUR("broker.enqueue.hour",MonitorRecord.class,"Broker每10秒入队条数"),
    BROKER_ENQUEUE_SIZE_HOUR("broker.enqueue.size.hour",MonitorRecord.class,"Broker每十秒入队大小"),

    BROKER_ENQUEUE_DAY("broker.enqueue.day",MonitorRecord.class,"Broker每10秒入队条数"),
    BROKER_ENQUEUE_SIZE_DAY("broker.enqueue.size.day",MonitorRecord.class,"Broker每十秒入队大小"),

    BROKER_DEQUEUE_SECOND("broker.dequeue.second",MonitorRecord.class,"broker出队累计条数"),
    BROKER_DEQUEUE_SIZE_SECOND("broker.dequeue.size.second",MonitorRecord.class,"Broker出队累计大小"),

    BROKER_DEQUEUE_MINUTE("broker.dequeue.minute",MonitorRecord.class,"broker出队累计条数"),
    BROKER_DEQUEUE_SIZE_MINUTE("broker.dequeue.size.minute",MonitorRecord.class,"Broker出队累计大小"),

    BROKER_DEQUEUE_HOUR("broker.dequeue.hour",MonitorRecord.class,"broker出队累计条数"),
    BROKER_DEQUEUE_SIZE_HOUR("broker.dequeue.size.hour",MonitorRecord.class,"Broker出队累计大小"),

    BROKER_DEQUEUE_DAY("broker.dequeue.day",MonitorRecord.class,"broker出队累计条数"),
    BROKER_DEQUEUE_SIZE_DAY("broker.dequeue.size.day",MonitorRecord.class,"Broker出队累计大小"),

    BROKER_DEQUEUE("broker.dequeue",MonitorRecord.class,"broker出队累计条数"),
    BROKER_DEQUEUE_SIZE("broker.dequeue.size",MonitorRecord.class,"Broker出队累计大小"),

    BROKER_PENDING("broker.pending",MonitorRecord.class,"Broker当前积压数量"),
    BROKER_MAXJOURNALOFFSET("broker.max.journal.offset",MonitorRecord.class,"Broker当前最大日志位置"),
    BROKER_JOURNAL_BADFILE("broker.journal.badfile",MonitorRecord.class,"从节点是否有坏块"),
    APP_CONNECTION("app.connection",MonitorRecord.class,"应用连接数(报警)"),
    APP_PRODUCE("app.produce",MonitorRecord.class,"应用生产者数量(报警)"),
    APP_CONSUME("app.consume",MonitorRecord.class,"应用消费者数量(报警)"),

//    APP_ENQUEUE("app.enqueue",MonitorRecord.class,"应用入队累计"),
//    APP_DEQUEUE("app.dequeue",MonitorRecord.class,"应用出队累计"),

    APP_ENQUEUE_SECOND("app.enqueue.second",MonitorRecord.class,"10秒应用入队累计"),
    APP_DEQUEUE_SECOND("app.dequeue.second",MonitorRecord.class,"10秒应用出队累计"),

    APP_ENQUEUE_MINUTE("app.enqueue.minute",MonitorRecord.class,"一分钟应用入队累计"),
    APP_DEQUEUE_MINUTE("app.dequeue.minute",MonitorRecord.class,"一分钟应用出队累计"),

    APP_ENQUEUE_HOUR("app.enqueue.hour",MonitorRecord.class,"一小时应用入队累计"),
    APP_DEQUEUE_HOUR("app.dequeue.hour",MonitorRecord.class,"一小时应用出队累计"),

    APP_ENQUEUE_DAY("app.enqueue.day",MonitorRecord.class,"一天应用入队累计"),
    APP_DEQUEUE_DAY("app.dequeue.day",MonitorRecord.class,"一天应用出队累计"),


    APP_ENQUEUE_SIZE_SECOND("app.enqueue.size.second",MonitorRecord.class,"10秒应用入队流量累计"),
    APP_DEQUEUE_SIZE_SECOND("app.dequeue.size.second",MonitorRecord.class,"10秒应用出队流量累计"),

    APP_ENQUEUE_SIZE_MINUTE("app.enqueue.size.minute",MonitorRecord.class,"一分钟应用入队流量累计"),
    APP_DEQUEUE_SIZE_MINUTE("app.dequeue.size.minute",MonitorRecord.class,"一分钟应用出队流量累计"),

    APP_ENQUEUE_SIZE_HOUR("app.enqueue.size.hour",MonitorRecord.class,"一小时应用入队流量累计"),
    APP_DEQUEUE_SIZE_HOUR("app.dequeue.size.hour",MonitorRecord.class,"一小时应用出队流量累计"),

    APP_ENQUEUE_SIZE_DAY("app.enqueue.size.day",MonitorRecord.class,"一天应用入队流量累计"),
    APP_DEQUEUE_SIZE_DAY("app.dequeue.size.day",MonitorRecord.class,"一天应用出队流量累计"),

    APP_PENDING("app.pending",MonitorRecord.class,"应用消费积压(报警)"),

    TOPIC_SESSION("topic.session",TopicSession.class,"主题应用实例"),

    TOPIC_ENQUEUE_SECOND("topic.enqueue.second",MonitorRecord.class,"10秒主题入队累计"),
    TOPIC_DEQUEUE_SECOND("topic.dequeue.second",MonitorRecord.class,"10秒主题出队累计"),

    TOPIC_ENQUEUE_MINUTE("topic.enqueue.minute",MonitorRecord.class,"一分钟主题入队累计"),
    TOPIC_DEQUEUE_MINUTE("topic.dequeue.minute",MonitorRecord.class,"一分钟主题出队累计"),

    TOPIC_ENQUEUE_HOUR("topic.enqueue.hour",MonitorRecord.class,"一小时主题入队累计"),
    TOPIC_DEQUEUE_HOUR("topic.dequeue.hour",MonitorRecord.class,"一小时主题出队累计"),

    TOPIC_ENQUEUE_DAY("topic.enqueue.day",MonitorRecord.class,"一天主题入队累计"),
    TOPIC_DEQUEUE_DAY("topic.dequeue.day",MonitorRecord.class,"一天主题出队累计"),

    TOPIC_ENQUEUE_SIZE_SECOND("topic.enqueue.size.second",MonitorRecord.class,"10秒主题入队流量累计"),
    TOPIC_DEQUEUE_SIZE_SECOND("topic.dequeue.size.second",MonitorRecord.class,"10秒主题出队流量累计"),

    TOPIC_ENQUEUE_SIZE_MINUTE("topic.enqueue.size.minute",MonitorRecord.class,"一分钟主题入队流量累计"),
    TOPIC_DEQUEUE_SIZE_MINUTE("topic.dequeue.size.minute",MonitorRecord.class,"一分钟主题出队流量累计"),

    TOPIC_ENQUEUE_SIZE_HOUR("topic.enqueue.size.hour",MonitorRecord.class,"一小时主题入队流量累计"),
    TOPIC_DEQUEUE_SIZE_HOUR("topic.dequeue.size.hour",MonitorRecord.class,"一小时主题出队流量累计"),

    TOPIC_ENQUEUE_SIZE_DAY("topic.enqueue.size.day",MonitorRecord.class,"一天主题入队流量累计"),
    TOPIC_DEQUEUE_SIZE_DAY("topic.dequeue.size.day",MonitorRecord.class,"一天主题出队流量累计");




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

    public String getDesc() {
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
