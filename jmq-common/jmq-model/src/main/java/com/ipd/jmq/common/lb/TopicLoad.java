package com.ipd.jmq.common.lb;


import java.util.List;
import java.util.Map;

/**
 * TopicLoad
 *
 * @author luoruiheng
 * @since 8/4/16
 */
public class TopicLoad {
    Long time;
    /**
     * the name of an app
     */
    private String app;

    // Map<group: String, Map<type: String, List<clientId: String>>>
    private Map<String, Map<ClientType, List<String>>> loads;


    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public Map<String, Map<ClientType, List<String>>> getLoads() {
        return loads;
    }

    public void setLoads(Map<String, Map<ClientType, List<String>>> loads) {
        this.loads = loads;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "TopicLoad{" +
                "app='" + app + '\'' +
                ", loads=" + loads +
                ", time=" + loads +
                '}';
    }
}
