package me.yuanbin.kafka;

import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KafkaStreamRouterTest {

    @Test
    public void testProperties() {
        String appId = "application-id";
        String servers = "127.0.0.1:9092";
        Properties properties = KafkaStreamRouter.getKafkaProps(appId, servers);
        assertEquals(properties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG), appId);
        assertEquals(properties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG), servers);
    }
}
