package broker.telnet;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.beust.jcommander.JCommander;
import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.cluster.Permission;
import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.network.v3.command.telnet.AuthCmd;
import com.ipd.jmq.common.network.v3.command.telnet.MonitorCmd;
import com.ipd.jmq.common.network.v3.command.telnet.PermiQueryCmd;
import com.ipd.jmq.common.network.v3.command.telnet.TopicCmd;
import com.ipd.jmq.common.network.v3.protocol.telnet.TelnetHeader;
import com.ipd.jmq.common.network.v3.protocol.telnet.TelnetRequest;
import com.ipd.jmq.common.network.v3.protocol.telnet.TelnetResponse;
import com.ipd.jmq.common.telnet.Commands;
import com.ipd.jmq.common.telnet.TelnetCode;
import com.ipd.jmq.common.telnet.TelnetResult;
import com.ipd.jmq.common.network.v3.netty.telnet.param.MonitorParam;
import com.ipd.jmq.common.network.v3.netty.telnet.TelnetClient;
import com.ipd.jmq.common.network.v3.netty.telnet.param.PermiQueryParam;
import com.ipd.jmq.common.network.ClientConfig;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.handler.telnet.TopicHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;

/**
 * Created by zhangkepeng on 16-11-24.
 */
public class TelnetCommandTest {

    private static final Logger logger = LoggerFactory.getLogger(TelnetCommandTest.class);
    private ClientConfig config = new ClientConfig();
    private TelnetClient client = new TelnetClient(config, null, null, null);
    private Transport transport = null;

    @Before
    public void init() throws Exception {
        if (!client.isStarted()) {
            client.start();
        }
        transport = client.createTransport(new InetSocketAddress(InetAddress.getByName("192.168.1.5"), 10088));
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
            logger.info("message:" + message);
            //TelnetResult telnetResult = JSONObject.parseObject(message, TelnetResult.class);
            //short status = telnetResult.getStatus();
            //Assert.assertEquals(TelnetCode.NO_ERROR, status);
            //String statStr = telnetResult.getMessage();
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            if (null != transport) transport.stop();
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
            logger.error(e.getMessage());
        } finally {
            if (null != transport) transport.stop();
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
            String message1 = ((TelnetResponse) transport.sync(command1).getPayload()).getMessage();
            TelnetResult telnetResult1 = JSONObject.parseObject(message1, TelnetResult.class);
            short status1 = telnetResult1.getStatus();
            Assert.assertEquals(TelnetCode.NO_ERROR, status1);
            String permissionStr1 = telnetResult1.getMessage();
            Permission permission1 = JSONObject.parseObject(permissionStr1, Permission.class);
            Assert.assertEquals(Permission.FULL, permission1);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            if (null != transport) transport.stop();
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
            String message1 = ((TelnetResponse) transport.sync(command1).getPayload()).getMessage();
            TelnetResult telnetResult1 = JSONObject.parseObject(message1, TelnetResult.class);
            short status1 = telnetResult1.getStatus();
            Assert.assertEquals(TelnetCode.NO_ERROR, status1);
            String permissionStr1 = telnetResult1.getMessage();
            Permission permission1 = JSONObject.parseObject(permissionStr1, Permission.class);
            Assert.assertEquals(Permission.FULL, permission1);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            if (null != transport) transport.stop();
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
            cmdBuilder.append(Commands.SEPARATOR).append("-u").append(Commands.SEPARATOR).
                    append("jmq").append(Commands.SEPARATOR).append("-p").append(Commands.SEPARATOR).append("jmq");
            logger.info("auth request:" + cmdBuilder);
            AuthCmd cmd = new AuthCmd(cmdBuilder);
            TelnetRequest telnetRequest = new TelnetRequest(cmd.cmd());
            Command command = new Command(TelnetHeader.Builder.request(), telnetRequest);
            String message = ((TelnetResponse) transport.sync(command).getPayload()).getMessage();
            logger.info("auth response" + message);
            TelnetResult telnetResult = JSON.parseObject(message, TelnetResult.class);
            Assert.assertEquals(telnetResult.getStatus(), 3);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * 在注册中心获得topic信息(全量)
     */
    @Test
    public void getTopic() {
        try {
            StringBuilder cmdBuilder = new StringBuilder(Commands.TOPIC);
            cmdBuilder.append(Commands.SEPARATOR).append(TopicHandler.GET);
            logger.info("get topic request " + cmdBuilder);
            TopicCmd cmd = new TopicCmd(cmdBuilder);
            TelnetRequest telnetRequest = new TelnetRequest(cmd.cmd());
            Command command = new Command(TelnetHeader.Builder.request(), telnetRequest);
            String message = ((TelnetResponse) transport.sync(command).getPayload()).getMessage();
            logger.info("get topic " + message);
            TelnetResult telnetResult = JSON.parseObject(message, TelnetResult.class);
            List<TopicConfig> topicConfigs = JSON.parseArray(telnetResult.getMessage(), TopicConfig.class);
            logger.info("topicConfigs size is {}", topicConfigs == null ? 0 : topicConfigs.size());
            Assert.assertEquals(0, telnetResult.getStatus());
        } catch (Exception e) {

        }
    }

    /**
     * 在注册中心设在topic(全量)
     */
    @Test
    public void setTopic() {
        try {
//            TopicConfig config = new TopicConfig();
//            String topicsInfo  = "[{\"archive\":false,\"consumers\":{\"szmApp4Consumer\":{}},\"groups\":[\"jmq102\"]," +
//                    "\"importance\":1, \"producers\":{\"szmapp\":{}},\"queues\":5,\"topic\":\"szm_pc\",\"type\":\"TOPIC\"}]";
            String topicsInfo = getCommandInfoFromLocal("topic_init.json");
            StringBuilder cmdBuilder = new StringBuilder(Commands.TOPIC);
            cmdBuilder.append(Commands.SEPARATOR).append(TopicHandler.SET).append(Commands.SEPARATOR).append(topicsInfo);
            logger.info("set topic request: " + cmdBuilder);
            TopicCmd cmd = new TopicCmd(cmdBuilder);
            TelnetRequest telnetRequest = new TelnetRequest(cmd.cmd());
            Command command = new Command(TelnetHeader.Builder.request(), telnetRequest);
            String message = ((TelnetResponse) transport.sync(command).getPayload()).getMessage();
            logger.info("response topic result: " + message);
            TelnetResult telnetResult = JSON.parseObject(message, TelnetResult.class);
            Assert.assertEquals(0, telnetResult.getStatus());
        } catch (Exception e) {

        }
    }

    /**
     * 在注册中心获得broker信息(全量)
     */
    @Test
    public void getBroker() {
        try {
            StringBuilder cmdBuilder = new StringBuilder(Commands.BROKER);
            cmdBuilder.append(Commands.SEPARATOR).append(TopicHandler.GET);
            logger.info("get broker request:" + cmdBuilder);
            TopicCmd cmd = new TopicCmd(cmdBuilder);
            TelnetRequest telnetRequest = new TelnetRequest(cmd.cmd());
            Command command = new Command(TelnetHeader.Builder.request(), telnetRequest);
            String message = ((TelnetResponse) transport.sync(command).getPayload()).getMessage();
            logger.info("get broker:" + message);
            TelnetResult telnetResult = JSON.parseObject(message, TelnetResult.class);
            List<BrokerConfig> brokerConfigs = JSON.parseArray(telnetResult.getMessage(), BrokerConfig.class);
            logger.info("brokerConfigs size is {}", brokerConfigs == null ? 0 : brokerConfigs.size());
            Assert.assertEquals(0, telnetResult.getStatus());
        } catch (Exception e) {

        }
    }

    /**
     * 在注册中心设置broker信息(全量)
     */
    @Test
    public void setBroker() {
        try {
//            BrokerConfig config = new BrokerConfig();
//            String topicInfo  = "[{\"alias\":\"jmq100_m\",\"dataCenter\":9,\"ip\":\"10.42.0.141\",\"permission\":\"FULL\",\"port\":50088,\"retryType\":\"DB\",\"syncMode\":\"SYNCHRONOUS\"}," +
//                    "{\"alias\":\"jmq101_m\",\"dataCenter\":10,\"ip\":\"192.168.0.103\",\"permission\":\"NONE\",\"port\":50088,\"retryType\":\"DB\",\"syncMode\":\"SYNCHRONOUS\"}," +
//                    "{\"alias\":\"jmq102_m\",\"dataCenter\":9,\"ip\":\"192.168.1.5\",\"permission\":\"FULL\",\"port\":50088,\"retryType\":\"DB\",\"syncMode\":\"SYNCHRONOUS\"}]";
            String brokersInfo = getCommandInfoFromLocal("broker_init.json");
            StringBuilder cmdBuilder = new StringBuilder(Commands.BROKER);
            cmdBuilder.append(Commands.SEPARATOR).append(TopicHandler.SET).append(Commands.SEPARATOR).append(brokersInfo);
            logger.info("set broker request: " + cmdBuilder);
            TopicCmd cmd = new TopicCmd(cmdBuilder);
            TelnetRequest telnetRequest = new TelnetRequest(cmd.cmd());
            Command command = new Command(TelnetHeader.Builder.request(), telnetRequest);
            String message = ((TelnetResponse) transport.sync(command).getPayload()).getMessage();
            logger.info("response broker result: " + message);
            TelnetResult telnetResult = JSON.parseObject(message, TelnetResult.class);
            Assert.assertEquals(0, telnetResult.getStatus());
        } catch (Exception e) {

        }
    }

    /**
     * 初始化集群的配置信息
     */
    private String getCommandInfoFromLocal(String fileName) {
        URL url = TelnetCommandTest.class.getClassLoader().getResource(fileName);
        InputStream in = null;
        BufferedReader bufferedReader = null;
        InputStreamReader inputStreamReader = null;
        String commandInfo = "";
        if (url != null) {
            logger.info("find " + fileName + " resource");
            try {
                in = url.openStream();
                inputStreamReader = new InputStreamReader(in, "UTF-8");
                bufferedReader = new BufferedReader(inputStreamReader);
                String lineStr;
                while ((lineStr = bufferedReader.readLine()) != null) {
                    commandInfo += lineStr;
                }

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (bufferedReader != null) {
                    try {
                        bufferedReader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (inputStreamReader != null) {
                    try {
                        inputStreamReader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else {
            logger.info("no find " + fileName + " resource");
        }
        return commandInfo;
    }
}
