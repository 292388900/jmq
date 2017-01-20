package com.ipd.jmq.common.lb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TopicSession
 *
 * @author luoruiheng
 * @since 8/4/16
 */
public class TopicSession {

    /**
     * the name of topic
     */
    private String topic;



    /**
     * topic on which broker-group
     */
    private String group;

    /**
     * IP of topic on what brokers
     */
    private String brokerIP;



    /**
     * is the master of one broker-group?
     */
    private boolean isMaster;

    /**
     * timestamp
     */
    private long time;

    /**
     * the sessions list of a client-app
     */
    private Map<String, Map<ClientType, List<ClientSession>>> appSessions;

    public TopicSession() {

    }

    public TopicSession(String topic, String group, String brokerIP, boolean isMaster, long time) {
        this.topic = topic;
        this.group = group;
        this.brokerIP = brokerIP;
        this.isMaster = isMaster;
        this.time = time;
    }

    public void addSession(ClientType type, String app, String connectionId) {
        ClientSession clientSession = new ClientSession(type, app, connectionId);

        if (null == this.appSessions.get(app)) {
            List<ClientSession> list = new ArrayList<ClientSession>();
            Map<ClientType, List<ClientSession>> map = new HashMap<ClientType, List<ClientSession>>();
            list.add(clientSession);
            map.put(type, list);
            this.appSessions.put(app, map);
        } else {
            if (null == this.appSessions.get(app).get(type)) {
                List<ClientSession> list = new ArrayList<ClientSession>();
                list.add(clientSession);
                this.getAppSessions().get(app).put(type, list);
            } else {
                this.appSessions.get(app).get(type).add(clientSession);
            }
        }

    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getBrokerIP() {
        return brokerIP;
    }

    public void setBrokerIP(String brokerIP) {
        this.brokerIP = brokerIP;
    }

    public boolean isMaster() {
        return isMaster;
    }

    public void setMaster(boolean master) {
        isMaster = master;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public Map<String, Map<ClientType, List<ClientSession>>> getAppSessions() {
        return appSessions;
    }

    public void setAppSessions(Map<String, Map<ClientType, List<ClientSession>>> appSessions) {
        this.appSessions = appSessions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicSession that = (TopicSession) o;

        if (isMaster != that.isMaster) return false;
        if (time != that.time) return false;
        if (!topic.equals(that.topic)) return false;
        if (!group.equals(that.group)) return false;
        if (!brokerIP.equals(that.brokerIP)) return false;
        return appSessions.equals(that.appSessions);

    }

    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + group.hashCode();
        result = 31 * result + brokerIP.hashCode();
        result = 31 * result + (isMaster ? 1 : 0);
        result = 31 * result + (int) (time ^ (time >>> 32));
        result = 31 * result + appSessions.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TopicSession{" +
                "topic='" + topic + '\'' +
                ", group='" + group + '\'' +
                ", brokerIP='" + brokerIP + '\'' +
                ", isMaster=" + isMaster +
                ", appSessions=" + appSessions +
                '}';
    }
}
