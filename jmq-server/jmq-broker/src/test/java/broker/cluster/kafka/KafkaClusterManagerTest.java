package broker.cluster.kafka;

import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.common.network.ServerConfig;
import com.ipd.jmq.registry.Registry;
import com.ipd.jmq.registry.zookeeper.ZKRegistry;
import com.ipd.jmq.toolkit.URL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by zhangkepeng on 16-8-9.
 */
public class KafkaClusterManagerTest {

    private static ServerConfig serverConfig;
    private static BrokerConfig config;
    private static Registry registry;

    @BeforeClass
    public static void init() throws Exception {
        serverConfig = ServerConfig.Builder.create().port(9092).ip("127.0.0.1").build();
        registry = new ZKRegistry(URL.valueOf("zookeeper://localhost"));
        registry.start();
        while (!registry.isConnected()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

            }
        }
        config = new BrokerConfig();
        config.setRegistry(registry);
        config.setServerConfig(serverConfig);
    }

    @Test
    public void test() throws Exception {
        /*final KafkaClusterManager kafkaClusterManager = new KafkaClusterManager(storeService, config);
        kafkaClusterManager.doStart();
        int count = 0;
        while (registry.isConnected()) {
            count ++;
            if (count < 5) {
                Thread.sleep(1000);
            }else {
                break;
            }
        }
        kafkaClusterManager.stop();*/
    }

    @AfterClass
    public static void clear() {
        registry.stop();
    }
}
