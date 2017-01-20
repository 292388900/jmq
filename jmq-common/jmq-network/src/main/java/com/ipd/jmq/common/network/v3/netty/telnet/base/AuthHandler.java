package com.ipd.jmq.common.network.v3.netty.telnet.base;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.ipd.jmq.common.network.v3.netty.telnet.param.AuthParam;
import com.ipd.jmq.common.network.command.telnet.Commands;
import com.ipd.jmq.common.network.command.telnet.TelnetCode;
import com.ipd.jmq.common.network.command.telnet.TelnetResult;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.common.network.protocol.telnet.TelnetHandler;
import com.ipd.jmq.common.network.protocol.telnet.TelnetHeader;
import com.ipd.jmq.common.network.protocol.telnet.TelnetRequest;
import com.ipd.jmq.common.network.protocol.telnet.TelnetResponse;
import com.ipd.jmq.toolkit.lang.Preconditions;

/**
 * Created by zhangkepeng on 16-11-29.
 */
public class AuthHandler implements TelnetHandler<Transport> {

    // 管理员用户
    private String adminUser;
    // 管理员密码
    private String adminPassword;

    // 只做访问控制，无加密解密操作
    public AuthHandler(String adminUser, String adminPassword) {
        Preconditions.checkArgument(adminUser != null, "adminUser can not be null");
        Preconditions.checkArgument(adminPassword != null, "adminPassword can not be null");
        this.adminUser = adminUser;
        this.adminPassword = adminPassword;
    }

    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {
        TelnetResult telnetResult = new TelnetResult();
        short status = TelnetCode.AUTH_ERROR;
        String text = TelnetCode.ERROR_TEXT.get(TelnetCode.AUTH_ERROR);
        try {
            // 权限认证
            TelnetRequest payload = (TelnetRequest) command.getPayload();
            String args[] = payload.getArgs();
            AuthParam authParam = new AuthParam();
            if (args != null && args.length > 0) {
                new JCommander(authParam, args);
            }

            String user = authParam.getUser();
            String password = authParam.getPassword();

            if (adminUser.equals(user) && adminPassword.equals(password)) {
                // 认证成功，加入已认证集合
                TelnetChannelHandler.AUTHENTICATED_TRANSPORTS.add(transport);

                status = TelnetCode.AUTH_SUCCESS;
                text = TelnetCode.ERROR_TEXT.get(TelnetCode.AUTH_SUCCESS);

            }
            telnetResult.setType(Commands.AUTH);
        } catch (ParameterException e) {
            // 命令参数异常，进行异常处理
            status = TelnetCode.PARAM_ERROR;
            text = TelnetCode.ERROR_TEXT.get(TelnetCode.PARAM_ERROR);
        }

        telnetResult.setStatus(status);
        telnetResult.setMessage(text);
        return new Command(TelnetHeader.Builder.response(),
                            new TelnetResponse(JSON.toJSONString(telnetResult), true,true));
    }

    /**
     * 命令
     *
     * @return
     */
    @Override
    public String command() {
        return Commands.AUTH;
    }

    /**
     * 帮助信息
     *
     * @return 帮助信息
     */
    @Override
    public String help() {
        return "usage:auth -u XXX -p XXX";
    }
}
