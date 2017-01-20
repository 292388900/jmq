package com.ipd.jmq.server.broker.handler.telnet;

import com.alibaba.fastjson.JSONObject;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.ipd.jmq.common.network.protocol.telnet.TelnetHandler;
import com.ipd.jmq.common.network.protocol.telnet.TelnetHeader;
import com.ipd.jmq.common.network.protocol.telnet.TelnetRequest;
import com.ipd.jmq.common.network.protocol.telnet.TelnetResponse;
import com.ipd.jmq.common.network.command.telnet.Commands;
import com.ipd.jmq.common.network.command.telnet.TelnetCode;
import com.ipd.jmq.common.network.command.telnet.TelnetResult;
import com.ipd.jmq.common.network.v3.netty.telnet.base.TelnetChannelHandler;
import com.ipd.jmq.common.network.v3.netty.telnet.param.MonitorParam;
import com.ipd.jmq.server.broker.handler.telnet.utils.ReflectUtils;
import com.ipd.jmq.server.broker.handler.telnet.utils.TelnetMap;
import com.ipd.jmq.server.broker.monitor.BrokerMonitor;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.toolkit.lang.Preconditions;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;


/**
 * Created by zhangkepeng on 16-11-24.
 */
public class MonitorHandler implements TelnetHandler<Transport> {

    // 管理员用户
    private String adminUser;
    // 管理员密码
    private String adminPassword;

    protected BrokerMonitor brokerMonitor;

    public MonitorHandler(BrokerMonitor brokerMonitor, String adminUser, String adminPassword) {
        Preconditions.checkArgument(brokerMonitor != null, "brokerMonitor can not be null");
        Preconditions.checkArgument(adminUser != null, "adminUser can not be null");
        Preconditions.checkArgument(adminPassword != null, "adminPassword can not be null");
        this.brokerMonitor = brokerMonitor;
        this.adminPassword = adminPassword;
        this.adminUser = adminUser;
    }

    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {
        String text;
        short status = TelnetCode.NO_ERROR;
        TelnetResult telnetResult = new TelnetResult();
        Method invokeMethod = null;
        TelnetRequest payload = (TelnetRequest) command.getPayload();
        String args[] = payload.getArgs();
        MonitorParam monitorParam = new MonitorParam();
        try {
            new JCommander(monitorParam, args);
        } catch (ParameterException e) {
            // 命令参数异常，进行异常处理
            status = TelnetCode.PARAM_ERROR;
            text = TelnetCode.ERROR_TEXT.get(TelnetCode.PARAM_ERROR);
            telnetResult.setMessage(text);
            telnetResult.setStatus(status);
            return new Command(TelnetHeader.Builder.response(), telnetResult);
        }

        // 判断是否包含认证
        if (monitorParam.getUser() != null && monitorParam.getPassword() != null) {
            String user = monitorParam.getUser();
            String password = monitorParam.getPassword();
            if (adminUser.equals(user) && adminPassword.equals(password)) {
                // 认证成功，加入已认证集合
                TelnetChannelHandler.AUTHENTICATED_TRANSPORTS.add(transport);
            } else {
                text = TelnetCode.ERROR_TEXT.get(TelnetCode.AUTH_ERROR);
                telnetResult.setStatus(TelnetCode.AUTH_ERROR);
                telnetResult.setMessage(text);
                telnetResult.setMessage(monitorParam.getInvoke());
                return new Command(TelnetHeader.Builder.response(), telnetResult);
            }
        }

        String invoke = monitorParam.getInvoke();

        if (invoke == null) {
            status = TelnetCode.PARAM_ERROR;
            text = TelnetCode.ERROR_TEXT.get(TelnetCode.PARAM_ERROR);
        } else {
            Object[] invokeFields = TelnetMap.PARAM_METHOD_MAP.get(monitorParam.getInvoke());
            Object[] invokeParams = null;
            try {
                if (invokeFields != null) {
                    invokeParams = new Object[invokeFields.length];
                    for (int field = 0; field < invokeFields.length; field++) {
                        invokeParams[field] = ((Field)invokeFields[field]).get(monitorParam);
                    }
                }
                String methodName = TelnetMap.METHOD_MAP.get(monitorParam.getInvoke());
                invokeMethod = ReflectUtils.findMethod(BrokerMonitor.class, methodName, invokeParams);
                if (invokeMethod == null) {
                    text = TelnetCode.ERROR_TEXT.get(TelnetCode.NO_METHOD_EXCEPTION);
                    status = TelnetCode.NO_METHOD_EXCEPTION;
                } else {
                    Object object = invokeMethod.invoke(brokerMonitor, invokeParams);
                    text = JSONObject.toJSONString(object);
                }
            } catch (InvocationTargetException e) {
                text = TelnetCode.ERROR_TEXT.get(TelnetCode.INVOCATION_EXCEPTION);
                status = TelnetCode.INVOCATION_EXCEPTION;
            } catch (IllegalAccessException e) {
                text = TelnetCode.ERROR_TEXT.get(TelnetCode.ACCESS_EXCEPTION);
                status = TelnetCode.ACCESS_EXCEPTION;
            }
        }

        telnetResult.setStatus(status);
        telnetResult.setMessage(text);
        telnetResult.setType(invoke);
        return new Command(TelnetHeader.Builder.response(),
                new TelnetResponse(JSONObject.toJSONString(telnetResult), true,true));
    }

    /**
     * 命令
     *
     * @return
     */
    @Override
    public String command() {
        return Commands.MONITOR;
    }

    /**
     * 帮助信息
     *
     * @return 帮助信息
     */
    @Override
    public String help() {
        return null;
    }
}
