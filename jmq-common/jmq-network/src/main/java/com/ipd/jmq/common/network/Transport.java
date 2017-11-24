package com.ipd.jmq.common.network;


import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.common.network.v3.command.CommandCallback;

import java.net.SocketAddress;

/**
 * 传输接口，负责进行通信
 */
public interface Transport {
    String TRANSPORT = "transport";

    /**
     * 同步发送，需要应答
     *
     * @param command 命令
     * @return 应答命令
     * @throws TransportException
     */
    Command sync(Command command) throws TransportException;

    /**
     * 同步发送，需要应答
     *
     * @param command 命令
     * @param timeout 超时
     * @return 应答命令
     * @throws TransportException
     */
    Command sync(Command command, int timeout) throws TransportException;

    /**
     * 异步发送，需要应答
     *
     * @param command  命令
     * @param callback 回调
     * @throws TransportException
     */
    void async(Command command, CommandCallback callback) throws TransportException;

    /**
     * 异步发送，需要应答
     *
     * @param command  命令
     * @param timeout  超时
     * @param callback 回调
     * @throws TransportException
     */
    void async(Command command, int timeout, CommandCallback callback) throws TransportException;

    /**
     * 单向发送，不需要应答
     *
     * @param command 命令
     * @throws TransportException
     */
    void oneway(Command command) throws TransportException;

    /**
     * 单向发送，需要应答
     *
     * @param command 命令
     * @param timeout 超时
     * @throws TransportException
     */
    void oneway(Command command, int timeout) throws TransportException;

    /**
     * 应答
     *
     * @param request  请求
     * @param response 响应
     * @param callback 回调
     * @throws TransportException
     */
    void acknowledge(Command request, Command response, CommandCallback callback) throws TransportException;

    /**
     * 获取远端地址
     *
     * @return 远端地址
     */
    SocketAddress remoteAddress();

    /**
     * 获取本地地址
     *
     * @return 本地地址
     */
    SocketAddress localAddress();

    /**
     * 获取键值
     *
     * @param key 键
     * @return 键值
     */
    Object attr(String key);

    /**
     * 设置键值
     *
     * @param key   键
     * @param value 值对象
     */
    void attr(String key, Object value);

    /**
     * 停止
     */
    void stop();

}