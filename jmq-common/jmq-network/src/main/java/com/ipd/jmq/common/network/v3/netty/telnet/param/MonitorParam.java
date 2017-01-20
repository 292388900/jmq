package com.ipd.jmq.common.network.v3.netty.telnet.param;

import com.beust.jcommander.Parameter;
import com.ipd.jmq.common.cluster.ClusterRole;
import com.ipd.jmq.common.lb.TopicSession;
import com.ipd.jmq.common.model.OffsetInfo;
import com.ipd.jmq.common.monitor.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangkepeng on 16-11-29.
 * <p/>
 * 监控命令参数
 */
public class MonitorParam {
    public static final String TOPIC = "-t";
    public static final String APP = "-a";
    public static final String INVOKE = "-i";
    public static final String USER = "-u";
    public static final String PASSWORD = "-p";
    public static final String QUEUEID = "-q";
    public static final String COUNT = "-C";
    public static final String LONG = "-l";
    public static final String BOOL = "-b";
    public static final String CONSUMER = "-c";


    @Parameter(names = {"-t", "--topic"})
    private String topic;

    @Parameter(names = {"-a", "--app"})
    private String app;

    @Parameter(names = {"-i", "--invoke"}, required = true)
    private String invoke;

    @Parameter(names = {"-u", "--user"})
    private String user;

    @Parameter(names = {"-p", "--password"})
    private String password;

    @Parameter(names = {"-q", "--queueId"})
    private short queueId;

    @Parameter(names = {"-C", "--count"})
    private int count;

    @Parameter(names = {"-l", "--long"})
    private long value;

    @Parameter(names = {"-b", "--bool"})
    private boolean bool;

    @Parameter(names = {"-c", "--consumer"})
    private String consumer;

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public boolean isBool() {
        return bool;
    }

    public void setBool(boolean bool) {
        this.bool = bool;
    }

    public short getQueueId() {
        return queueId;
    }

    public void setQueueId(short queueId) {
        this.queueId = queueId;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getInvoke() {
        return invoke;
    }

    public void setInvoke(String invoke) {
        this.invoke = invoke;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getConsumer() {
        return consumer;
    }

    public void setConsumer(String consumer) {
        this.consumer = consumer;
    }

    public static enum IdentityType {

        /**
         * 监控类型
         */
        // 分片级别监控指令类型
        STAT("stat", BrokerStat.class),
        PERF("perf", BrokerPerf.class),
        EXCE("exec", BrokerExe.class),
        STATE("state", ReplicationState.class),
        REPS("reps", Boolean.class),
        ROLE("role", ClusterRole.class),
        VERSION("version", String.class),
        INFO("Info", String.class),
        CONFIG("config", String.class),
        // 主题级别监控指令类型
        ADD("add", String.class),
        METRICS("metrics", String.class),
        // 消费者级别监控指令类型
        SUBSCRIBE("subscribe", String.class),
        OFFSETINFO("offsetinfo", OffsetInfo.class),
        RESETOFFSET("resetoffset", Void.class),
        LOCATION("location", String.class),
        CLEANEXPIRE("cleanexpire", Boolean.class),
        VIEWMESSAGE("viewmessage", List.class),
        UNSUBSCRIBE("unsubscribe", Void.class),
        NOACKBLOCKS("noackblocks", String.class),
        // 会话级别监控
        SESSION("session", TopicSession.class),
        CLIENT("client", Client.class),
        CLOSEPRODUCER("closeproducer", Void.class),
        CLOSECONSUMER("closeconsumer", Void.class);

        private static Map<String, IdentityType> instructionsMap = new HashMap<String, IdentityType>();

        private String identityType;

        private Class cls;

        public String getIdentityType() {
            return identityType;
        }

        public Class getCls() {
            return cls;
        }

        IdentityType(String identityType, Class cls) {
            this.identityType = identityType;
            this.cls = cls;
        }

        public static IdentityType findIdentityType(String instruction) {
            if (instructionsMap.isEmpty()) {
                synchronized (instructionsMap) {
                    if (instructionsMap.isEmpty()) {
                        for (IdentityType identity : IdentityType.values()) {
                            instructionsMap.put(identity.getIdentityType(), identity);
                        }
                    }
                }
            }
            return instructionsMap.get(instruction);
        }
    }
}
