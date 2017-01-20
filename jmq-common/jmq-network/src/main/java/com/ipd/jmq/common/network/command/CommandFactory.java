package com.ipd.jmq.common.network.command;

/**
 * Created by hexiaofeng on 16-6-22.
 */
public interface CommandFactory {
    /**
     * 根据头部创建命令
     *
     * @param header 头部
     */
    Command create(Header header);
}
