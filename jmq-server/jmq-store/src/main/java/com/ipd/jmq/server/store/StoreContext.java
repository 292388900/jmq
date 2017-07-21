package com.ipd.jmq.server.store;

import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.model.JournalLog;

import java.util.List;

/**
 * Created by guoliang5 on 2017/3/1.
 */
public class StoreContext<T extends JournalLog> {
    final private String topic;
    final private String app;
    final private List<T> logs;
    private PutResult result;
    private Store.ProcessedCallback callback;

    public StoreContext(String topic, String app, List<T> logs, Store.ProcessedCallback callback) {
        this.topic = topic;
        this.app = app;
        this.logs = logs;
        this.callback = callback;
        this.result = new PutResult(JMQCode.CN_UNKNOWN_ERROR, null);
    }

    public String getTopic() {
        return topic;
    }

    public String getApp() {
        return app;
    }

    public List<T> getLogs() {
        return logs;
    }

    public PutResult getResult() {
        return result;
    }

    public void setResult(PutResult result) {
        this.result = result;
    }

    public Store.ProcessedCallback getCallback() {
        return callback;
    }

    public void setCallback(Store.ProcessedCallback callback) {
        this.callback = callback;
    }
}
