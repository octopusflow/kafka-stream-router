package me.yuanbin.kafka.task.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import me.yuanbin.common.util.TypesafeConfigUtil;
import me.yuanbin.kafka.predicate.SensorsValuePredicate;
import me.yuanbin.kafka.task.AbstractTask;
import me.yuanbin.kafka.task.ETLTask;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SensorsValueRouterTask extends AbstractTask implements ETLTask {

    private static final Logger logger = LoggerFactory.getLogger(SensorsValueRouterTask.class);

    @Override
    public void build(StreamsBuilder builder) {
        final KStream<String, JsonNode> sourceStream = builder.stream(sourceTopics, sensorsConsumed);
        for (Map.Entry<String, ConfigValue> entry : sinkTopicRouter.entrySet()) {
            // TypesafeConfig would make xxx.xxx as a unique key, so we should split it with .
            String rawSinkTopic = entry.getKey();
            String sinkTopic = rawSinkTopic.split("\\.")[0];
            logger.info("build stream task with sink.topic {}", sinkTopic);
            Config sinkTopicConfig = sinkTopicRouter.getConfig(sinkTopic);
            List<String> whitelist = TypesafeConfigUtil.getStringList(sinkTopicConfig, WHITELIST);
            sourceStream
                    .filter(new SensorsValuePredicate(whitelist))
                    .to(sinkTopic, stringJsonProduced);
        }
    }
}
