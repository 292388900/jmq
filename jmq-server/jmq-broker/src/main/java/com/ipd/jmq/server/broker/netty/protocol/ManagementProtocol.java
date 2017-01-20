package com.ipd.jmq.server.broker.netty.protocol;

import com.ipd.jmq.common.network.netty.protocol.telnet.TelnetProtocol;
import com.ipd.jmq.common.network.v3.netty.telnet.base.AuthenticatedEnterHandler;
import com.ipd.jmq.common.network.v3.netty.telnet.base.TelnetChannelHandler;
import com.ipd.jmq.common.network.CommandHandlerFactory;
import com.ipd.jmq.common.network.netty.codec.Encoder;
import com.ipd.jmq.common.network.netty.protocol.telnet.TelnetDecoder;
import com.ipd.jmq.common.network.netty.protocol.telnet.TelnetEncoder;
import io.netty.channel.ChannelHandler;

/**
 * Created by zhangkepeng on 16-11-25.
 *
 * 在middleware TelnetProtocol基础上替换了解码类
 */
public class ManagementProtocol extends TelnetProtocol {

    public ManagementProtocol(CommandHandlerFactory factory) {
        super(factory);
    }

    public ManagementProtocol(CommandHandlerFactory factory, String prompt, int maxHistorySize, Encoder encoder) {
        super(factory, prompt, maxHistorySize, encoder);
    }

    @Override
    public ChannelHandler[] channelHandlers() {
        return new ChannelHandler[]{
                new TelnetDecoder(factory, prompt, maxHistorySize, new AuthenticatedEnterHandler(factory)),
                new TelnetEncoder(prompt, encoder),
                new TelnetChannelHandler()};
    }
}
