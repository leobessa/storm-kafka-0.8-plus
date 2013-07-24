package storm.kafka;

import backtype.storm.Config;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.TestingServer;
import kafka.javaapi.consumer.SimpleConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import storm.kafka.trident.IBrokerReader;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created with IntelliJ IDEA.
 * User: leobessa
 * Date: 7/24/13
 * Time: 4:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class DynamicPartitionConnectionsTest {

    private DynamicBrokersReader dynamicBrokersReader;
    private String masterPath = "/brokers";
    private String topic = "testing";
    private CuratorFramework zookeeper;
    private TestingServer server;

    @Before
    public void setUp() throws Exception {
        server = new TestingServer();
        String connectionString = server.getConnectString();
        Map conf = new HashMap();
        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 1000);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 4);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 5);
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zookeeper = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        zookeeper.start();
    }

    @After
    public void tearDown() throws Exception {
        server.close();
    }

    @Test
    public void testRegister() {
        String topic = "topic1";
        String clientId = "clientA";
        BrokerHosts hosts = new ZkHosts(server.getConnectString());
        KafkaConfig config = new KafkaConfig(hosts, topic, clientId);
        IBrokerReader brokerReader = null;
        final DynamicPartitionConnections subject = new DynamicPartitionConnections(config, brokerReader);
        int partition = 0;
        HostPort host = new HostPort("example.com", 9092);
        final SimpleConsumer consumer = subject.register(host, partition);
        assertThat(consumer.clientId(), is(clientId));
    }
}
