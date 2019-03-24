package me.yuanbin.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

public class KafkaStreamRouterTest {

    @Test
    public void testConfig() {
        System.out.println("Serdes String class: " + Serdes.String().getClass());

//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }
}
