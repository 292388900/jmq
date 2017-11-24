package com.ipd.jmq.common.network.v3.session;

import com.ipd.jmq.common.network.Language;
import com.ipd.jmq.common.network.Transport;

/**
 * 连接会话
 */
public class Connection {
    // 通道
    private Transport transport;
    // 连接ID
    private String id;
    // 应用
    private String app;
    // 版本
    private String version;
    // 语言
    private Language language = Language.JAVA;
    // 客户端地址
    private byte[] address;
    // 服务端地址
    private byte[] serverAddress;

    public Connection() {
    }

    public Connection(String id, String app, String version, Language language, byte[] address, byte[] serverAddress) {
        this.id = id;
        this.app = app;
        this.version = version;
        this.language = language;
        this.address = address;
        this.serverAddress = serverAddress;
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getApp() {
        return this.app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getVersion() {
        return this.version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Language getLanguage() {
        return this.language;
    }

    public void setLanguage(Language language) {
        this.language = language;
    }

    public byte[] getAddress() {
        return this.address;
    }

    public void setAddress(byte[] address) {
        this.address = address;
    }

    public byte[] getServerAddress() {
        return serverAddress;
    }

    public void setServerAddress(byte[] serverAddress) {
        this.serverAddress = serverAddress;
    }

    public Transport getTransport() {
        return transport;
    }

    public void setTransport(Transport transport) {
        this.transport = transport;
    }
}