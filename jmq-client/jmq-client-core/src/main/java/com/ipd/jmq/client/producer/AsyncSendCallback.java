package com.ipd.jmq.client.producer;

import com.ipd.jmq.common.network.v3.command.PutMessage;
import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.common.network.command.CommandCallback;

/**
 * 异步回调接口
 *
 * Created by zhangkepeng on 15-9-14.
 */
public abstract class AsyncSendCallback implements CommandCallback {
    /**
     * 成功
     *
     * @param request  请求命令
     * @param response 应答命令
     */
    public void onSuccess(Command request, Command response) {
        success((PutMessage) request.getPayload(), response);
    }

    /**
     * 出现异常
     *
     * @param request 请求命令
     * @param cause   异常
     */
    public void onException(Command request, Throwable cause) {
        exception((PutMessage) request.getPayload(), cause);
    }

    /**
     * 发送成功
     * @param putMessage 生产消息
     * @param response 应答命令
     */
    public abstract void success(PutMessage putMessage, Command response);

    /**
     * 发送异常
     * @param putMessage 生产消息
     * @param cause 异常
     */
    public abstract void exception(PutMessage putMessage, Throwable cause);
}
