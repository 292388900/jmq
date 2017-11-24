package com.ipd.jmq.common.network;

import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.lang.LifeCycle;

import java.net.SocketAddress;

/**
 * 客户端接口
 * Created by hexiaofeng on 16-6-22.
 */
public interface Client extends LifeCycle {


    /**
     * 创建连接，阻塞直到成功或失败
     *
     * @param address 地址
     * @return 通道
     * @throws TransportException
     */
    Transport createTransport(final String address) throws TransportException;

    /**
     * 创建连接，阻塞直到成功或失败
     *
     * @param address           地址
     * @param connectionTimeout 连接超时
     * @return 通道
     * @throws TransportException
     */
    Transport createTransport(final String address, final long connectionTimeout) throws TransportException;

    /**
     * 创建连接，阻塞直到成功或失败
     *
     * @param address 地址
     * @return 通道
     * @throws TransportException
     */
    Transport createTransport(final SocketAddress address) throws TransportException;

    /**
     * 创建连接
     *
     * @param address           地址
     * @param connectionTimeout 连接超时
     * @return 通道
     * @throws TransportException
     */
    Transport createTransport(final SocketAddress address, final long connectionTimeout) throws TransportException;

    /**
     * 增加监听器
     *
     * @param listener 监听器
     */
    void addListener(EventListener<TransportEvent> listener);

    /**
     * 删除监听器
     *
     * @param listener 监听器
     */
    void removeListener(EventListener<TransportEvent> listener);
}
