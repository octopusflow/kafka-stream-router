package me.yuanbin.kafka.task;

import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import me.yuanbin.kafka.predicate.MaxwellKeyPredicate;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MaxwellKeyTask extends AbstractTask {

    private static final Logger logger = LoggerFactory.getLogger(MaxwellKeyTask.class);

    // JSON untyped Serde
    private final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer());
    private final Consumed<JsonNode, byte[]> consumed = Consumed.with(jsonSerde, Serdes.ByteArray());
    private final Produced<JsonNode, byte[]> produced = Produced.with(jsonSerde, Serdes.ByteArray());

    @Override
    public void build(StreamsBuilder builder) {
        final KStream<JsonNode, byte[]> sourceStream = builder.stream(sourceTopics, consumed);

        for (Map.Entry<String, ConfigValue> entry : sinkTopicsConfig.entrySet()) {
            String sinkTopic = entry.getKey();
            Config topicConfig = sinkTopicsConfig.getConfig(sinkTopic);
            String filterRegex = "";
            if (topicConfig.hasPath(KEY_FILTER_REGEX)) {
                filterRegex = getRegex(topicConfig, KEY_FILTER_REGEX);
            } else if (topicConfig.hasPath(VALUE_FILTER_REGEX)) {
                filterRegex = getRegex(topicConfig, VALUE_FILTER_REGEX);
            }
            logger.info("use filter regex: {} for sink topic: {}", filterRegex, sinkTopic);
            sourceStream.filter(new MaxwellKeyPredicate(filterRegex)).to(sinkTopic, produced);
        }
    }
}
