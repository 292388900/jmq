package com.ipd.jmq.common.network.v3.netty;

import com.ipd.jmq.common.network.Transport;
import io.netty.channel.Channel;

/**
 * Created by hexiaofeng on 16-6-22.
 */
public interface ChannelTransport extends Transport {

    /**
     * 返回通道
     *
     * @return
     */
    Channel channel();
}
