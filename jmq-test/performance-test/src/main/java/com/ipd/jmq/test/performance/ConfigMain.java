package com.ipd.jmq.test.performance;

import com.ipd.jmq.registry.zookeeper.ZKRegistry;
import com.ipd.jmq.test.performance.config.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * Created by zhangkepeng on 2017/1/9.
 */
public class ConfigMain {
    private static final Logger logger = LoggerFactory.getLogger(ConfigMain.class);

    public void config(){
        String brokers = null;
        String topics = null;
        String zookeeper = null;
        Properties p = new Properties();
        InputStream in = null;
        try {

            URL url = ConfigMain.class.getClassLoader().getResource("config.properties");
            if(url == null){
                url = ConfigMain.class.getClassLoader().getResource("conf/config.properties");
            }
            in = url.openStream();
            p.load(in);

            brokers = p.getProperty("config.brokers");
            topics = p.getProperty("config.topics");
            zookeeper = p.getProperty("config.zookeeper");
            ConfigUtil.registry = new ZKRegistry(com.ipd.jmq.toolkit.URL.valueOf(zookeeper));
            ConfigUtil.registry.start();
            ConfigUtil.brokerUpdate(brokers);
            ConfigUtil.partitionUpdate(topics);
        } catch (Exception e) {
            logger.error("",e);
        } finally {
            if(in != null){
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            ConfigUtil.registry.stop();
        }
    }

    public static void main(String[] args) {
        ConfigMain cm = new ConfigMain();
        cm.config();
    }
}
