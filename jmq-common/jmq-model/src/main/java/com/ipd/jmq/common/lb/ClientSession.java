package com.ipd.jmq.common.lb;

/**
 * ClientSession
 *
 * @author luoruiheng
 * @since 8/4/16
 */
public class ClientSession {

    /**
     * type of client（CONSUMER or PRODUCER）
     */
    private ClientType type;

    /**
     * the name of client app
     */
    private String app;

    /**
     * the connection Id
     *
     */
    private String connectionId;


    public ClientSession(ClientType type, String connectionId) {
        this.type = type;
        this.connectionId = connectionId;
    }

    public ClientSession(ClientType type, String app, String connectionId) {
        this.type = type;
        this.app  = app;
        this.connectionId = connectionId;
    }

    public ClientType getType() {
        return type;
    }

    public void setType(ClientType type) {
        this.type = type;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(String connectionId) {
        this.connectionId = connectionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClientSession that = (ClientSession) o;

        if (type != that.type) return false;
        if (!app.equals(that.app)) return false;
        return connectionId.equals(that.connectionId);

    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + app.hashCode();
        result = 31 * result + connectionId.hashCode();
        return result;
    }
}
