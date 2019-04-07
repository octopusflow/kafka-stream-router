package me.yuanbin.kafka.task;

import com.fasterxml.jackson.databind.JsonNode;
import me.yuanbin.common.config.AppConfigFactory;

import com.typesafe.config.Config;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static me.yuanbin.common.util.TypesafeConfigUtil.getStringList;

public abstract class AbstractTask {

    private static final Logger logger = LoggerFactory.getLogger(AbstractTask.class);

    private static final String SOURCE_TOPICS = "source.topics";
    private static final String SINK_TOPIC_ROUTER = "sink.topic.router";
    protected static final String WHITELIST = "whitelist";
    protected static final String ENABLE_WHITELIST_REGEX = "enableWhitelistRegex";

    private static final Config appConfig = AppConfigFactory.load();
    protected final Config taskConfig = appConfig.getConfig("task." + this.getClass().getSimpleName());
    protected final List<String> sourceTopics = getStringList(taskConfig, SOURCE_TOPICS);
    protected final Config sinkTopicRouter = taskConfig.getConfig(SINK_TOPIC_ROUTER);

    // JSON untyped Serde
    private final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer());
    protected final Consumed<JsonNode, JsonNode> jsonConsumed = Consumed.with(jsonSerde, jsonSerde);
    protected final Produced<JsonNode, JsonNode> jsonProduced = Produced.with(jsonSerde, jsonSerde);
    // old kafka 0.8.2 does not have timestamp, use process time instead
    protected final Consumed<String, JsonNode> sensorsConsumed = Consumed.with(Serdes.String(), jsonSerde)
            .withTimestampExtractor(new WallclockTimestampExtractor());
    protected final Consumed<String, JsonNode> stringJsonConsumed = Consumed.with(Serdes.String(), jsonSerde);
    protected final Produced<String, JsonNode> stringJsonProduced = Produced.with(Serdes.String(), jsonSerde);
    protected final Consumed<JsonNode, byte[]> jsonBytesConsumed = Consumed.with(jsonSerde, Serdes.ByteArray());
    protected final Consumed<byte[], byte[]> bytesConsumed = Consumed.with(Serdes.ByteArray(), Serdes.ByteArray());
    protected final Produced<JsonNode, byte[]> jsonBytesProduced = Produced.with(jsonSerde, Serdes.ByteArray());
    protected final Produced<byte[], byte[]> bytesProduced = Produced.with(Serdes.ByteArray(), Serdes.ByteArray());
}
