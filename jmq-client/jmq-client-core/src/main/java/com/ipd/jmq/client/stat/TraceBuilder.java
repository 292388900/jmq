package com.ipd.jmq.client.stat;

import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.common.network.v3.netty.NettyClient;
import com.ipd.jmq.toolkit.URL;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.plugin.PluginUtil;
import com.ipd.jmq.toolkit.service.Service;

import java.util.List;

/**
 * 客户端性能统计插件构建器
 */
public class TraceBuilder extends Service implements Trace {
    // 性能插件
    private List<Trace> traces;
    // 应用
    private String app;
    // 客户端通信
    private NettyClient client;
    // 连接管理器
    private TransportManager transportManager;

    @Override
    protected void validate() throws Exception {
        super.validate();
        if (client == null) {
            throw new IllegalStateException("client can not be null.");
        }
        this.app = ((TransportConfig) client.getConfig()).getApp();
        if (traces == null) {
            // 加载插件
            traces = PluginUtil.loadPlugins(Trace.class);
        }
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        if (traces != null) {
            // 启动插件
            for (Trace trace : traces) {
                trace.setClient(client);
                trace.setTransportManager(transportManager);
                trace.start();
            }
        }
    }

    @Override
    protected void doStop() {
        Close.close(traces);
        super.doStop();
    }

    @Override
    public String getApp() {
        return app;
    }

    @Override
    public void begin(final TraceInfo info) {
        if (info == null || traces == null || traces.isEmpty()) {
            return;
        }
        // 开始统计
        for (Trace trace : traces) {
            trace.begin(info);
        }
    }

    @Override
    public void end(final TraceInfo info) {
        if (info == null || traces == null || traces.isEmpty()) {
            return;
        }
        // 结束统计
        for (Trace trace : traces) {
            trace.end(info);
        }
        info.clear();
    }

    @Override
    public void setClient(NettyClient client) {
        this.client = client;
    }

    @Override
    public void setTransportManager(TransportManager transportManager) {
        this.transportManager = transportManager;
    }

    @Override
    public String getType() {
        return "builder";
    }

    @Override
    public void setUrl(URL url) {

    }
}
