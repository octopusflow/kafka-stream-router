package me.yuanbin.kafka;

import com.google.common.base.Strings;
import com.typesafe.config.Config;
import me.yuanbin.common.config.AppConfigFactory;
import me.yuanbin.kafka.task.ETLTask;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class StreamTasksFactory {
    private static final Logger logger = LoggerFactory.getLogger(StreamTasksFactory.class);
    private static final String APPLICATION_ID_KEY = "application.id";
    private static final String DEFAULT_APPLICATION_ID = "kafka.application.id";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    private static final Config appConfig = AppConfigFactory.load();

    private static final Map<String, StreamsBuilder> tasksBuilder = new ConcurrentHashMap<>();

    private static Properties getKafkaProps(String appId, String bootstrapServers) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    private static Properties getKafkaProps(String appId) {
        String finalAppId = Strings.isNullOrEmpty(appId) ? appConfig.getString(DEFAULT_APPLICATION_ID) : appId;
        String bootstrapServers = appConfig.getString(DEFAULT_BOOTSTRAP_SERVERS);
        return getKafkaProps(finalAppId, bootstrapServers);
    }

    private static String getAppId(ETLTask etlTask) {
        final Config taskConfig = appConfig.getConfig("task." + etlTask.getClass().getSimpleName());
        if (taskConfig.hasPath(APPLICATION_ID_KEY)) {
            return taskConfig.getString(APPLICATION_ID_KEY);
        }
        return appConfig.getString(DEFAULT_APPLICATION_ID);
    }

    public static void load(ETLTask etlTask) {
        String appId = getAppId(etlTask);
        logger.info("load task {} with application.id {}", etlTask.getClass().getSimpleName(), appId);
        tasksBuilder.putIfAbsent(appId, new StreamsBuilder());
        etlTask.build(tasksBuilder.get(appId));
    }

    /**
     * start at the end of main entry
     */
    public static void start() {
        for (Map.Entry<String, StreamsBuilder> entry : tasksBuilder.entrySet()) {
            String appId = entry.getKey();
            StreamsBuilder taskBuilder = entry.getValue();
            Properties properties = getKafkaProps(appId);
            logger.info("start with application.id: {}", appId);
            KafkaStreams streams = new KafkaStreams(taskBuilder.build(), properties);
            streams.start();
        }
    }
}
