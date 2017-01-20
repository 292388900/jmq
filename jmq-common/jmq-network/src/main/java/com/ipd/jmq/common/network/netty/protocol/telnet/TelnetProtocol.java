package com.ipd.jmq.common.network.netty.protocol.telnet;

import com.ipd.jmq.common.network.CommandHandlerFactory;
import com.ipd.jmq.common.network.netty.Protocol;
import com.ipd.jmq.common.network.netty.codec.Encoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;

/**
 * Telnet协议
 * Created by hexiaofeng on 16-7-7.
 */
public class TelnetProtocol implements Protocol {

    // 提醒符号
    protected String prompt = ">";
    // 最大历史条数
    protected int maxHistorySize = 100;
    // 编码器
    protected Encoder encoder;
    // 命令处理器
    protected CommandHandlerFactory factory;

    public TelnetProtocol(CommandHandlerFactory factory) {
        if (factory == null) {
            throw new IllegalArgumentException("factory can not be null.");
        }
        this.factory = factory;
    }

    public TelnetProtocol(CommandHandlerFactory factory, String prompt, int maxHistorySize, Encoder encoder) {
        if (factory == null) {
            throw new IllegalArgumentException("factory can not be null.");
        } else if (prompt == null || prompt.isEmpty()) {
            throw new IllegalArgumentException("prompt can not be empty.");
        }
        this.factory = factory;
        this.prompt = prompt;
        this.maxHistorySize = maxHistorySize;
        this.encoder = encoder;
    }

    @Override
    public boolean support(final ByteBuf in) {
        int magic = in.getUnsignedShort(0);
        int low = (magic & 0xFFFF) >> 8;
        int high = magic & 0xFF;
        if (low == 255 && (236 <= high && high <= 254)) {
            // Telnet NVT命令
            return true;
        }
        return true;
    }

    @Override
    public boolean keeplive() {
        return true;
    }

    @Override
    public ChannelHandler[] channelHandlers() {
        return new ChannelHandler[]{new TelnetDecoder(factory, prompt, maxHistorySize),
                new TelnetEncoder(prompt, encoder)};
    }
}
