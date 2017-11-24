package com.ipd.jmq.server.broker;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.network.Ipv4;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * 启动
 */
public class Launcher {
    public static final String JMQ_PROPERTIES = "jmq.properties";
    public static final String NETTY_SERVER_PORT = "netty.server.port";
    public static final String BROKER_NAME = "broker.name";
    public static final String BROKER_PORT = "broker.port";
    public static final String BROKER_SERVICE = "brokerService";
    protected static Logger logger = LoggerFactory.getLogger(Launcher.class);

    public static void main(String[] args) throws Exception {
        String ip = Ipv4.getLocalIp();
        int port = 0;
        // 获取配置文件
        if (args != null && args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
            }
        }
        if (port <= 0) {
            port = getPortByConfig();
        }
        if (port <= 0) {
            port = getPortByIp(ip);
        }
        Broker broker = new Broker(ip, port);
        // 设置系统环境变量
        System.setProperty(BROKER_NAME, broker.getName());
        System.setProperty(BROKER_PORT, String.valueOf(port));
        // 重新加载log4j配置文件
        URL url = Launcher.class.getClassLoader().getResource("log4j.properties");
        PropertyConfigurator.configure(url);

        ApplicationContext ctx;
        try {
            if (logger.isInfoEnabled()) {
                logger.info("load spring xml...");
            }
            ctx = new ClassPathXmlApplicationContext("spring-*.xml");
        } catch (Exception e) {
            logger.error("load config error.", e);
            return;
        }
        final JMQBrokerService JMQBrokerService = (JMQBrokerService) ctx.getBean(BROKER_SERVICE);
        if (!JMQBrokerService.isStarted()) {
            try {
                if (logger.isInfoEnabled()) {
                    logger.info("brokerService will start...");
                }
                JMQBrokerService.start();
            } catch (Exception e) {
                logger.error("start broker service error.", e);
                return;
            }
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("system is being shutdown.");
                if (JMQBrokerService.isStarted()) {
                    JMQBrokerService.stop();
                }
            }
        });
    }

    /**
     * 从配置文件获取端口
     *
     * @return 端口号
     * @throws IOException
     */
    protected static int getPortByConfig() {
        InputStream stream = null;
        int port = 0;
        Properties properties;
        // 加载应用路径配置文件
        try {
            stream = Launcher.class.getClassLoader().getResourceAsStream(JMQ_PROPERTIES);
            properties = new Properties();
            properties.load(stream);
            // 读取端口
            port = Integer.parseInt(properties.getProperty(NETTY_SERVER_PORT));
        } catch (NumberFormatException ignored) {
            // 端口没用配置
        } catch (IOException ignored) {
        } finally {
            Close.close(stream);
        }
        return port;
    }

    /**
     * 根据IP来获取端口
     *
     * @param ip ip地址
     * @return
     */
    protected static int getPortByIp(String ip) {
        // 根据IP末位数字来判断
        String[] parts = ip.split("\\.");
        int part = Integer.parseInt(parts[3]);
        return part % 2 == 0 ? 50088 : 50099;
    }

}
