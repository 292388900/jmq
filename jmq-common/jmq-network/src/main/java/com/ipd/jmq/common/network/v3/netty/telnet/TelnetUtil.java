package com.ipd.jmq.common.network.v3.netty.telnet;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ipd.jmq.common.exception.BusinessException;
import com.ipd.jmq.common.network.ClientConfig;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.common.network.v3.command.telnet.AuthCmd;
import com.ipd.jmq.common.network.v3.command.telnet.MonitorCmd;
import com.ipd.jmq.common.network.v3.netty.telnet.param.AuthParam;
import com.ipd.jmq.common.network.v3.netty.telnet.param.MonitorParam;
import com.ipd.jmq.common.network.v3.protocol.telnet.TelnetHeader;
import com.ipd.jmq.common.network.v3.protocol.telnet.TelnetRequest;
import com.ipd.jmq.common.network.v3.protocol.telnet.TelnetResponse;
import com.ipd.jmq.common.telnet.Commands;
import com.ipd.jmq.common.telnet.TelnetCode;
import com.ipd.jmq.common.telnet.TelnetResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Created by gouyaoqing on 2016/12/21.
 */
public class TelnetUtil {
    private String telnetUser;
    private String telnetPass;
    private Logger logger = LoggerFactory.getLogger(TelnetUtil.class);

    public TelnetUtil(String telnetUser, String telnetPass) {
        this.telnetUser = telnetUser;
        this.telnetPass = telnetPass;
    }

    public <T> T sync(TelnetClient client, String ip, int port, MonitorParam.IdentityType identityType, Class<T> clazz, Object... params) throws Exception {
        Transport transport = client.createTransport(new InetSocketAddress(InetAddress.getByName(ip), port), 50000);
        if (client == null) {
            throw new BusinessException("telnet连接创建失败");
        }
        if (auth(transport) == 3) {
            StringBuilder builder = new StringBuilder(Commands.MONITOR);
            builder.append(Commands.SEPARATOR).append(MonitorParam.INVOKE).append(Commands.SEPARATOR).append(identityType.getIdentityType())
                    .append(Commands.SEPARATOR);
            if (params != null && params.length > 0) {
                for (Object param : params) {
                    builder.append(param);
                }
            }
            MonitorCmd monitorCmd = new MonitorCmd(builder);
            TelnetRequest telnetRequest = new TelnetRequest(monitorCmd.cmd());
            Command command = new Command(TelnetHeader.Builder.request(), telnetRequest);
            String message = ((TelnetResponse) transport.sync(command).getPayload()).getMessage();
//            logger.info(String.format("telnet %s:%s"), ip, message);
            TelnetResult telnetResult = JSONObject.parseObject(message, TelnetResult.class);
            short status = telnetResult.getStatus();
            if (TelnetCode.NO_ERROR == status && !Void.class.equals(clazz)) {
                return JSON.parseObject(telnetResult.getMessage(), clazz);
            }
        } else {
            throw new BusinessException(String.format("telnet认证失败:%s", ip));
        }

        return null;
    }

    public void oneway(TelnetClient client, String ip, int port, MonitorParam.IdentityType identityType, Object... params) throws Exception {
        Transport transport = client.createTransport(new InetSocketAddress(InetAddress.getByName(ip), port), 50000);
        if (client == null) {
            throw new BusinessException("telnet连接创建失败");
        }
        if (auth(transport) == 3) {
            StringBuilder builder = new StringBuilder(Commands.MONITOR);
            builder.append(Commands.SEPARATOR).append(MonitorParam.INVOKE).append(Commands.SEPARATOR).append(identityType.getIdentityType())
                    .append(Commands.SEPARATOR);
            if (params != null && params.length > 0) {
                for (Object param : params) {
                    builder.append(param);
                }
            }
            MonitorCmd monitorCmd = new MonitorCmd(builder);
            TelnetRequest telnetRequest = new TelnetRequest(monitorCmd.cmd());
            Command command = new Command(TelnetHeader.Builder.request(), telnetRequest);
            transport.oneway(command);
        } else {
            throw new BusinessException("telnet认证失败");
        }
    }

    public TelnetClient open() throws Exception {
        TelnetClient client = new TelnetClient(ClientConfig.Builder.create().build());
        if (!client.isStarted()) {
            client.start();
        }
        return client;
    }

    public void close(TelnetClient client) {
        if (client != null && !client.isStopped()) {
            client.stop();
        }
    }

    private int auth(Transport transport) throws Exception {
        StringBuilder builder = new StringBuilder(Commands.AUTH);
        builder.append(Commands.SEPARATOR).append(AuthParam.USER).append(Commands.SEPARATOR).append(this.telnetUser)
                .append(Commands.SEPARATOR).append(AuthParam.PASSWORD).append(Commands.SEPARATOR).append(this.telnetPass);
        AuthCmd cmd = new AuthCmd(builder);
        TelnetRequest telnetRequest = new TelnetRequest(cmd.cmd());
        Command command = new Command(TelnetHeader.Builder.request(), telnetRequest);
        String message = ((TelnetResponse) transport.sync(command).getPayload()).getMessage();
        TelnetResult telnetResult = JSON.parseObject(message, TelnetResult.class);
        return telnetResult.getStatus();
    }
}

