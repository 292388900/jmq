package broker.kafka.netty;

import com.ipd.jmq.common.network.kafka.command.KafkaCommandKeys;
import com.ipd.jmq.common.network.kafka.command.KafkaHeader;
import com.ipd.jmq.common.network.kafka.command.UpdateTopicsBrokerRequest;
import com.ipd.jmq.common.network.kafka.exception.KafkaException;
import com.ipd.jmq.common.network.kafka.netty.KafkaNettyClient;
import com.ipd.jmq.common.network.ClientConfig;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.v3.command.Command;
import org.junit.*;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Created by zhangkepeng on 16-12-7.
 */
public class KafkaClientTest {

    private KafkaNettyClient kafkaNettyClient;

    private ClientConfig config = new ClientConfig();

    @Before
    public void init() throws Exception {
        kafkaNettyClient = new KafkaNettyClient(config);
        kafkaNettyClient.start();
    }

    @After
    public void destroy() throws Exception {
        kafkaNettyClient.stop();
    }

    @Test
    public void testTranspornt() throws TransportException{
        SocketAddress brokerAddress = new InetSocketAddress("10.12.165.66", 50088);
        Transport transport = kafkaNettyClient.createTransport(brokerAddress, 2000L);
        try {
            transport.sync(command(), 5000);
        } finally {
            transport.stop();
        }
    }

    private Command command() {
        KafkaHeader kafkaHeader = KafkaHeader.Builder.request(KafkaCommandKeys.UPDATE_TOPICS_BROKER);
        kafkaHeader.setCommandKey(KafkaCommandKeys.UPDATE_TOPICS_BROKER);
        Command command = new Command();
        // 同步发送更新命令给Controller
        UpdateTopicsBrokerRequest payload = new UpdateTopicsBrokerRequest();
        if (payload != null) {
            // 构造请求
            payload.setClientId("test");
            payload.setTopic("test");
            payload.setCorrelationId(2);
            payload.setVersion((short)1);
            command.setPayload(payload);
        } else {
            throw new KafkaException("not supported command type");
        }
        command.setHeader(kafkaHeader);
        return command;
    }
}
