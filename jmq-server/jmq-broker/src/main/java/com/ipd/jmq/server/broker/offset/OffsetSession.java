package com.ipd.jmq.server.broker.offset;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.network.FailoverState;
import com.ipd.jmq.common.network.FailoverTransport;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.v3.command.Identity;
import com.ipd.jmq.common.network.v3.command.JMQHeader;
import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.common.network.command.Direction;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * 消费位置会话
 *
 * @author lindeqiang
 * @since 2016-09-14
 */
public class OffsetSession implements EventListener<FailoverState> {
    private static final Logger logger = LoggerFactory.getLogger(OffsetSession.class);
    //成员
    private Broker broker;
    //管理命令和异步请求数据传输通道
    private Transport transport;
    //复制状态
    private AtomicReference<Status> status = new AtomicReference<Status>(Status.DISCONNECTED);

    public OffsetSession(Broker broker, Transport transport) {
        this.broker = broker;
        this.transport = transport;
    }

    public OffsetSession(Broker broker, Transport transport, Status status) {
        this.broker = broker;
        this.transport = transport;
        this.status.set(status);
    }

    public Broker getBroker() {
        return this.broker;
    }

    public void setBroker(Broker broker) {
        this.broker = broker;
    }

    public Transport getTransport() {
        return transport;
    }

    public void setTransport(Transport transport) {
        this.transport = transport;
    }

    public boolean isConnected() {
        return Status.CONNECTED.equals(this.status.get());
    }

    public void setStatus(Status status) {
        this.status.set(status);
    }

    /**
     * 比较并更新
     *
     * @param expect 期望值
     * @param update 更新值
     * @return 成功标示
     */
    public boolean compareAndSet(Status expect, Status update) {
        return status.compareAndSet(expect, update);
    }

    /**
     * 关闭通道
     */
    public void close() {
        close(false);
    }

    /**
     * 关闭通道
     *
     * @param keepStatus 保留状态
     */
    public void close(final boolean keepStatus) {
        if (!keepStatus) {
            status.set(Status.DISCONNECTED);
        }
        if (transport != null) {
            transport.stop();
        }
    }

    @Override
    public void onEvent(FailoverState event) {
        synchronized (this) {
            if (event.equals(FailoverState.WEAK) || event.equals(FailoverState.DISCONNECTED)) {
                setStatus(Status.DISCONNECTED);
            } else if (event.equals(FailoverState.CONNECTED)) {
                boolean authenticated = false;
                try {
                    Identity identity = new Identity();
                    identity.setBroker(getBroker());
                    Command command = new Command(JMQHeader.Builder.create().type(identity.type()).direction(Direction.REQUEST).build(), identity);
                    Command response = transport.sync(command, 20000);
                    JMQHeader header = (JMQHeader) response.getHeader();
                    if (header.getStatus() == JMQCode.SUCCESS.getCode()) {
                        authenticated = true;
                    }
                } catch (Exception e) {
                    logger.warn("OffsetSession authentication failed", e);
                }

                if (authenticated) {
                    setStatus(Status.CONNECTED);
                } else {
                    ((FailoverTransport) transport).weak();
                }
            }
        }
    }

    enum Status {
        /**
         * 未连接
         */
        DISCONNECTED,
        /**
         * 已连接
         */
        CONNECTED
    }

}