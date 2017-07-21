package com.ipd.jmq.common.network;

import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.common.network.v3.command.CommandCallback;
import com.ipd.jmq.common.network.v3.netty.ChannelTransport;
import com.ipd.jmq.toolkit.concurrent.EventBus;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.exception.Abnormity;
import com.ipd.jmq.toolkit.lang.Online;
import com.ipd.jmq.toolkit.network.Ipv4;
import com.ipd.jmq.toolkit.retry.Retry;
import com.ipd.jmq.toolkit.retry.RetryPolicy;
import com.ipd.jmq.toolkit.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 故障切换客户端
 */
public class FailoverClient extends Service implements FailoverTransport {
    private static Logger logger = LoggerFactory.getLogger(FailoverClient.class);
    // 客户端连接
    protected Client client;
    // 故障切换策略
    protected FailoverPolicy failoverPolicy;
    // 重试策略
    protected RetryPolicy retryPolicy;
    // 连接通道
    protected Transport transport;
    // Netty事件监听器
    protected TransportListener transportListener = new TransportListener();
    // 当前连接地址
    protected String address;
    // 连接状态
    protected AtomicReference<FailoverState> state = new AtomicReference<FailoverState>(FailoverState.DISCONNECTED);
    // 连接事件处理器
    protected EventBus<ConnectEvent> connectEvents = new EventBus<ConnectEvent>("ConnectEvent");
    // 故障切换事件处理器
    protected EventBus<FailoverState> failoverEvents = new EventBus<FailoverState>("FailoverEvent");
    // 连接监听器
    protected ConnectListener connectListener = new ConnectListener();
    protected Map<String, Object> attrs = new HashMap<String, Object>(10);

    /**
     * 构造函数
     *
     * @param client         客户端
     * @param failoverPolicy 故障切换策略
     * @param retryPolicy    重连策略
     */
    public FailoverClient(Client client, FailoverPolicy failoverPolicy, RetryPolicy retryPolicy) {
        if (client == null) {
            throw new IllegalArgumentException("nettyClient can not be null");
        }
        if (failoverPolicy == null) {
            throw new IllegalArgumentException("failoverPolicy can not be null");
        }
        this.client = client;
        this.failoverPolicy = failoverPolicy;
        this.retryPolicy = retryPolicy == null ? new RetryPolicy(1000, 10 * 1000, 0, true, 1.2, 0) : retryPolicy;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        state.set(FailoverState.DISCONNECTED);
        client.addListener(transportListener);
        if (!client.isStarted()) {
            client.start();
        }
        failoverEvents.start();
        connectEvents.start();
        connectEvents.addListener(connectListener);
        connectEvents.add(new ConnectEvent(transport, EventType.RECONNECT));
    }

    @Override
    protected void doStop() {
        client.removeListener(transportListener);
        connectEvents.removeListener(connectListener);
        connectEvents.stop();
        failoverEvents.stop();
        state.set(FailoverState.DISCONNECTED);
        if (transport != null) {
            transport.stop();
        }
        super.doStop();
    }

    @Override
    public Command sync(final Command command) throws TransportException {
        checkState();
        return transport.sync(command);
    }

    @Override
    public Command sync(final Command command, final int timeout) throws TransportException {
        checkState();
        return transport.sync(command, timeout);
    }

    @Override
    public void async(final Command command, final CommandCallback callback) throws TransportException {
        checkState();
        transport.async(command, callback);
    }

    @Override
    public void async(final Command command, final int timeout, final CommandCallback callback) throws
            TransportException {
        checkState();
        transport.async(command, timeout, callback);
    }

    @Override
    public void oneway(final Command command) throws TransportException {
        checkState();
        transport.oneway(command);
    }

    @Override
    public void oneway(final Command command, final int timeout) throws TransportException {
        checkState();
        transport.oneway(command, timeout);
    }

    @Override
    public void acknowledge(final Command request, final Command response, final CommandCallback callback) throws
            TransportException {
        checkState();
        transport.acknowledge(request, response, callback);
    }

    @Override
    public FailoverState getState() {
        return state.get();
    }

    @Override
    public void addListener(final EventListener<FailoverState> listener) {
        if (failoverEvents.addListener(listener)) {
            if (isStarted()) {
                failoverEvents.add(state.get(), listener);
            }
        }
    }

    @Override
    public void removeListener(final EventListener<FailoverState> listener) {
        failoverEvents.removeListener(listener);
    }

    @Override
    public void weak() {
        if (state.compareAndSet(FailoverState.CONNECTED, FailoverState.WEAK)) {
            connectEvents.add(new ConnectEvent(transport, EventType.HEALTH));
            failoverEvents.add(FailoverState.WEAK);
        }
    }

    @Override
    public int getWeight() {
        return state.get() == FailoverState.CONNECTED ? 100 : 0;
    }

    @Override
    public SocketAddress remoteAddress() {
        if (transport != null) {
            return transport.remoteAddress();
        }
        return null;
    }

    @Override
    public SocketAddress localAddress() {
        if (transport != null) {
            return transport.localAddress();
        }
        return null;
    }

    @Override
    public Object attr(final String key) {
        return attrs.get(key);
    }

    @Override
    public void attr(final String key, final Object value) {
        attrs.put(key, value);
    }

    protected void checkState() throws TransportException {
        if (!isStarted()) {
            throw TransportException.IllegalStateException.build();
        }
        if (state.get() != FailoverState.CONNECTED) {
            throw TransportException.ConnectionException.build(address);
        }
    }

    /**
     * 健康检查
     *
     * @param transport 通道
     * @return 健康表示
     * @throws TransportException
     */
    protected boolean checkHealth(Transport transport) throws TransportException {
        return true;
    }

    /**
     * 重连
     *
     * @return 重连成功
     * @throws TransportException
     */
    protected void reconnect() throws TransportException {
        if (!isStarted()) {
            return;
        }
        // 获取要连接的地址
        String addr = failoverPolicy.getAddress(address);
        address = addr;
        if (addr == null || addr.isEmpty()) {
            return;
        }
        // 当前通道停止
        Transport current = transport;
        if (current != null) {
            current.stop();
        }
        // 创建新通道
        current = client.createTransport(addr);
        try {
            if (current == null) {
                throw TransportException.NoPermissionException.build();
            } else if (!isStarted()) {
                throw TransportException.IllegalStateException.build();
            } else if (!login(current)) {
                throw TransportException.NoPermissionException.build();
            } else if (!checkHealth(current)) {
                throw TransportException.NoPermissionException.build();
            }
            // 初始化发送命令
            transport = current;
            if (state.compareAndSet(FailoverState.DISCONNECTED, FailoverState.CONNECTED)) {
                failoverEvents.add(FailoverState.CONNECTED);
                logger.info("success connecting to " + Ipv4.toAddress(transport.remoteAddress()));
            }
        } catch (TransportException e) {
            if (current != null) {
                current.stop();
            }
            throw e;
        }
    }

    /**
     * 登录认证
     *
     * @param transport 通道
     * @return 是否认证成功
     * @throws TransportException
     */
    protected boolean login(final Transport transport) throws TransportException {
        return true;
    }

    /**
     * 连接事件
     */
    protected enum EventType {
        /**
         * 重连
         */
        RECONNECT,
        /**
         * 健康检查
         */
        HEALTH
    }

    /**
     * 通道事件
     */
    protected class ConnectEvent {
        Transport transport;
        EventType type;

        public ConnectEvent(Transport transport, EventType type) {
            this.transport = transport;
            this.type = type;
        }
    }

    /**
     * 监听通道事件
     */
    protected class TransportListener implements EventListener<TransportEvent> {
        @Override
        public void onEvent(final TransportEvent event) {
            if (!isStarted()) {
                return;
            }
            Transport current = transport;
            Transport transport = event.getTransport();
            if (current != null && transport != null &&
                    ((ChannelTransport) transport).channel() == ((ChannelTransport) current).channel()) {
                // 如果是当前通道
                switch (event.getType()) {
                    case CLOSE://关闭
                    case EXCEPTION://异常
                        state.set(FailoverState.DISCONNECTED);
                        connectEvents.add(new ConnectEvent(current, EventType.RECONNECT));
                        logger.info("disconnect from " + Ipv4.toAddress(transport.remoteAddress()));
                        break;
                }
            }
        }
    }

    /**
     * 重连监听器
     */
    protected class ConnectListener implements EventListener<ConnectEvent> {
        @Override
        public void onEvent(final ConnectEvent event) {
            if (!isStarted()) {
                return;
            }
            Transport current = transport;
            // 如果通道已经变化，则丢弃当前事件
            if (current == event.transport) {
                try {
                    //广播当前状态
                    failoverEvents.add(state.get());
                    // 持续重连
                    Retry.execute(retryPolicy, new ConnectTask(event));
                } catch (Exception ignored) {
                    // 每个异常都会调用onException，这里就不要再输出日志
                }

            }
        }
    }

    /**
     * 连接任务
     */
    protected class ConnectTask implements Callable<Boolean>, Online, Abnormity {
        ConnectEvent event;

        public ConnectTask(ConnectEvent event) {
            this.event = event;
        }

        @Override
        public Boolean call() throws Exception {
            if (event.type == EventType.RECONNECT) {
                reconnect();
            } else if (event.type == EventType.HEALTH) {
                Transport transport = event.transport;
                if (checkHealth(transport)) {
                    if (state.compareAndSet(FailoverState.WEAK, FailoverState.CONNECTED)) {
                        failoverEvents.add(FailoverState.CONNECTED);
                        logger.info("success reconnecting to " + Ipv4.toAddress(transport.remoteAddress()));
                    }
                } else {
                    // 改用重连，切换到其它Broker
                    if (state.compareAndSet(FailoverState.WEAK, FailoverState.DISCONNECTED)) {
                        event.type = EventType.RECONNECT;
                        failoverEvents.add(FailoverState.DISCONNECTED);
                    }
                    throw TransportException.NoPermissionException.build(Ipv4.toAddress(transport.remoteAddress()));
                }
            }
            return Boolean.TRUE;
        }

        @Override
        public boolean isStarted() {
            return FailoverClient.this
                    .isStarted() && transport == event.transport && (event.type != EventType.HEALTH || state
                    .get() == FailoverState.WEAK);
        }

        @Override
        public boolean onException(final Throwable e) {
            String remoteIp = null;
            Transport transport = FailoverClient.this.transport;
            if (transport != null) {
                remoteIp = Ipv4.toAddress(transport.remoteAddress());
            }
            if (remoteIp == null) {
                remoteIp = address;
            }
            if (e instanceof TransportException) {
                logger.error(e.getMessage());
            } else {
                logger.error(e.getMessage() + ",ip=" + remoteIp, e);
            }
            return true;
        }
    }
}
