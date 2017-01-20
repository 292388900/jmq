package broker.telnet;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.beust.jcommander.JCommander;
import com.ipd.jmq.common.cluster.Permission;
import com.ipd.jmq.common.network.v3.command.telnet.AuthCmd;
import com.ipd.jmq.common.network.v3.command.telnet.MonitorCmd;
import com.ipd.jmq.common.network.v3.command.telnet.PermiQueryCmd;
import com.ipd.jmq.common.network.protocol.telnet.TelnetHeader;
import com.ipd.jmq.common.network.protocol.telnet.TelnetRequest;
import com.ipd.jmq.common.network.protocol.telnet.TelnetResponse;
import com.ipd.jmq.common.network.command.telnet.Commands;
import com.ipd.jmq.common.network.command.telnet.TelnetCode;
import com.ipd.jmq.common.network.command.telnet.TelnetResult;
import com.ipd.jmq.common.network.v3.netty.telnet.param.MonitorParam;
import com.ipd.jmq.common.network.v3.netty.telnet.TelnetClient;
import com.ipd.jmq.common.network.v3.netty.telnet.param.PermiQueryParam;
import com.ipd.jmq.common.network.ClientConfig;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.command.Command;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Created by zhangkepeng on 16-11-24.
 */
public class TelnetCommandTest {

    private static final Logger loger = LoggerFactory.getLogger(TelnetCommandTest.class);
    private ClientConfig config = new ClientConfig();
    private TelnetClient client = new TelnetClient(config, null, null, null);
    private Transport transport = null;

    @Before
    public void init() throws Exception{
        if(!client.isStarted()){
            client.start();
        }
        transport = client.createTransport(new InetSocketAddress(InetAddress.getByName("10.12.136.217"), 10088));
        auth();
    }

    @After
    public void destory() {
        if (client.isStarted()) {
            client.stop();
        }
    }

    /**
     * 参数对应MonitorParam中的参数
     */
    @Test
    public void testTelnetMonitor() {
        try {
            StringBuilder cmdBuilder = new StringBuilder(Commands.MONITOR);
            cmdBuilder.append(Commands.SEPARATOR).append("-i").append(Commands.SEPARATOR)
                    .append(MonitorParam.IdentityType.STAT.getIdentityType());
            MonitorCmd monitorCmd = new MonitorCmd(cmdBuilder);
            TelnetRequest telnetRequest = new TelnetRequest(monitorCmd.cmd());
            Command command = new Command(TelnetHeader.Builder.request(), telnetRequest);
            String message = ((TelnetResponse) transport.sync(command).getPayload()).getMessage();
            TelnetResult telnetResult = JSONObject.parseObject(message, TelnetResult.class);
            short status = telnetResult.getStatus();
            Assert.assertEquals(TelnetCode.NO_ERROR, status);
            String statStr = telnetResult.getMessage();
            System.out.println("ss"+statStr);
        } catch (Exception e) {
            loger.error(e.getMessage());
        } finally {
            if(null!=transport)transport.stop();
        }
    }

    /**
     * 参数对应PermiQueryParam中的参数
     */
    @Test
    public void testTelnetBrokerPermissionQuery() {
        try {
            // 分片权限请求
            StringBuilder cmdBuilder = new StringBuilder(Commands.PERMIQUERY);
            cmdBuilder.append(Commands.SEPARATOR).append("-l").append(Commands.SEPARATOR).append(PermiQueryParam.Level.BROKER.getLevel());
            PermiQueryCmd permiQueryCmd = new PermiQueryCmd(cmdBuilder);
            TelnetRequest telnetRequest = new TelnetRequest(permiQueryCmd.cmd());
            Command command = new Command(TelnetHeader.Builder.request(), telnetRequest);
            String message = ((TelnetResponse) transport.sync(command).getPayload()).getMessage();
            TelnetResult telnetResult = JSONObject.parseObject(message, TelnetResult.class);
            short status = telnetResult.getStatus();
            Assert.assertEquals(TelnetCode.NO_ERROR, status);
            String permissionStr = telnetResult.getMessage();
            Permission permission = JSONObject.parseObject(permissionStr, Permission.class);
            Assert.assertEquals(Permission.FULL, permission);
        } catch (Exception e) {
            loger.error(e.getMessage());
        } finally {
            if(null!=transport)transport.stop();
        }
    }

    @Test
    public void testTelnetTopicPermissionQuery() {
        try {
            // 主题权限请求
            StringBuilder builder = new StringBuilder(Commands.PERMIQUERY);
            builder.append(Commands.SEPARATOR).append("-l").append(PermiQueryParam.Level.TOPIC.getLevel()).
                    append(Commands.SEPARATOR).append("-t").append("test");
            PermiQueryCmd permiQueryCmd1 = new PermiQueryCmd(builder);
            TelnetRequest telnetRequest1 = new TelnetRequest(permiQueryCmd1.cmd());
            Command command1 = new Command(TelnetHeader.Builder.request(), telnetRequest1);
            String message1 = ((TelnetResponse)transport.sync(command1).getPayload()).getMessage();
            TelnetResult telnetResult1 = JSONObject.parseObject(message1, TelnetResult.class);
            short status1 = telnetResult1.getStatus();
            Assert.assertEquals(TelnetCode.NO_ERROR, status1);
            String permissionStr1 = telnetResult1.getMessage();
            Permission permission1 = JSONObject.parseObject(permissionStr1, Permission.class);
            Assert.assertEquals(Permission.FULL, permission1);
        } catch (Exception e) {
            loger.error(e.getMessage());
        } finally {
            if(null!=transport)transport.stop();
        }
    }

    @Test
    public void testTelnetAppPerssionQuery() {
        try {
            // 主题权限请求
            StringBuilder builder = new StringBuilder(Commands.PERMIQUERY);
            builder.append(Commands.SEPARATOR).append("-l").append(PermiQueryParam.Level.APP.getLevel()).
                    append(Commands.SEPARATOR).append("-t").append("test").
                    append(Commands.SEPARATOR).append("-a").
                    append(Commands.SEPARATOR).append("test");
            PermiQueryCmd permiQueryCmd1 = new PermiQueryCmd(builder);
            TelnetRequest telnetRequest1 = new TelnetRequest(permiQueryCmd1.cmd());
            Command command1 = new Command(TelnetHeader.Builder.request(), telnetRequest1);
            String message1 = ((TelnetResponse)transport.sync(command1).getPayload()).getMessage();
            TelnetResult telnetResult1 = JSONObject.parseObject(message1, TelnetResult.class);
            short status1 = telnetResult1.getStatus();
            Assert.assertEquals(TelnetCode.NO_ERROR, status1);
            String permissionStr1 = telnetResult1.getMessage();
            Permission permission1 = JSONObject.parseObject(permissionStr1, Permission.class);
            Assert.assertEquals(Permission.FULL, permission1);
        } catch (Exception e) {
            loger.error(e.getMessage());
        } finally {
            if(null!=transport)transport.stop();
        }
    }

    @Test
    public void testParseCommand() {
        TelnetRequest request = new TelnetRequest("monitor -t topic");
        MonitorParam monitorParam = new MonitorParam();
        String[] argv = request.getArgs();
        new JCommander(monitorParam, argv);
        Assert.assertEquals(monitorParam.getTopic(), "topic");
    }

    private void auth() {
        try {
            StringBuilder cmdBuilder = new StringBuilder(Commands.AUTH);
            cmdBuilder.append(Commands.AUTH).append(Commands.SEPARATOR).append("-u").append(Commands.SEPARATOR).
                    append("jmq").append(Commands.SEPARATOR).append("-p").append(Commands.SEPARATOR).append("jmq");
            AuthCmd cmd = new AuthCmd(cmdBuilder);
            TelnetRequest telnetRequest = new TelnetRequest(cmd.cmd());
            Command command = new Command(TelnetHeader.Builder.request(), telnetRequest);
            String message = ((TelnetResponse) transport.sync(command).getPayload()).getMessage();
            TelnetResult telnetResult = JSON.parseObject(message, TelnetResult.class);
            Assert.assertEquals(telnetResult.getStatus(), 3);
        } catch (Exception e) {
            loger.error(e.getMessage());
        }
    }
}
