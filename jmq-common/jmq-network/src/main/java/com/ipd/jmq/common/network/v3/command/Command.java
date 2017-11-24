package com.ipd.jmq.common.network.v3.command;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 命令
 * Created by hexiaofeng on 16-6-22.
 */
public class Command implements Releasable {
    // 头
    protected Header header;
    // 数据包
    protected Object payload;
    // 是否已经是否了
    protected AtomicBoolean released = new AtomicBoolean(false);
    // 附加属性
    protected Object object;

    public Command() {
    }

    public Command(Header header, Object payload) {
        this.header = header;
        this.payload = payload;
    }

    public Header getHeader() {
        return header;
    }

    public void setHeader(Header header) {
        this.header = header;
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    @Override
    public void release() {
        if (payload != null) {
            if (payload instanceof Releasable) {
                if (released.compareAndSet(false, true)) {
                    ((Releasable) payload).release();
                }
            }
        }
    }

    /**
     * 构造器
     */
    public static class Builder {

        protected Command command = new Command();

        public Builder() {
        }

        public Builder(Command command) {
            this.command = command;
        }

        public static Builder build() {
            return new Builder();
        }

        public static Builder build(final Command command) {
            return new Builder(command);
        }

        public Builder header(final Header header) {
            command.setHeader(header);
            return this;
        }

        public Builder payload(final Object payload) {
            command.setPayload(payload);
            return this;
        }
    }

    public interface Callback {
        void execute(Object object);
    }

}
