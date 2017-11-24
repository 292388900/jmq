package com.ipd.jmq.common.network.v3.netty.telnet.base;

import com.alibaba.fastjson.JSON;
import com.ipd.jmq.common.network.v3.netty.telnet.param.AuthParam;
import com.ipd.jmq.common.network.v3.protocol.telnet.*;
import com.ipd.jmq.common.telnet.Commands;
import com.ipd.jmq.common.telnet.TelnetCode;
import com.ipd.jmq.common.telnet.TelnetResult;
import com.ipd.jmq.common.network.CommandHandlerFactory;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.v3.command.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhangkepeng on 16-11-25.
 */
public class AuthenticatedEnterHandler extends EnterHandler {

    private static final Logger logger = LoggerFactory.getLogger(AuthenticatedEnterHandler.class);

    protected EnterHandler enterHandler;
    // 命令执行器
    protected CommandHandlerFactory factory;

    public AuthenticatedEnterHandler(CommandHandlerFactory factory) {
        super(factory);
        if (factory == null) {
            throw new IllegalArgumentException("factory can not be null.");
        }
        this.factory = factory;
        this.enterHandler = new EnterHandler(factory);
    }


    @Override
    public Command process(Transport transport, Command command) throws TransportException {
        TelnetInput input = (TelnetInput) transport.attr(TelnetInput.INPUT);
        try {
            if (command != null) {
                if (TelnetChannelHandler.AUTHENTICATED_TRANSPORTS.contains(transport)) {
                    // 已经认证逻辑处理
                    return enterHandler.process(transport, command);
                } else {
                    // 权限认证

                    TelnetRequest payload = (TelnetRequest) command.getPayload();

                    TelnetRequest telnetRequest = new TelnetRequest(payload.getMessage(), input.getInput() + payload.getInput());
                    TelnetResult telnetResult = new TelnetResult();
                    String commandType = telnetRequest.getCommand();
                    String commandInput = telnetRequest.getInput();
                    String passwordParam = AuthParam.PASSWORD;
                    String userParam = AuthParam.USER;
                    if (telnetRequest != null
                            && commandType != null
                            && (commandType.equals(Commands.AUTH)
                                || (-1 != commandInput.indexOf(passwordParam)
                                && -1 != commandInput.indexOf(userParam)))) {
                        return enterHandler.process(transport, command);
                    } else {
                        telnetResult.setMessage(TelnetCode.ERROR_TEXT.get(TelnetCode.NO_AUTH));
                        telnetResult.setStatus(TelnetCode.NO_AUTH);
                        String jsonResult = JSON.toJSONString(telnetResult);
                        return new Command(TelnetHeader.Builder.response(), new TelnetResponse(jsonResult, true, true));
                    }
                }
            }
            return null;
        } finally {
            if (input != null) {
                input.delete();
            }
        }
    }
}
