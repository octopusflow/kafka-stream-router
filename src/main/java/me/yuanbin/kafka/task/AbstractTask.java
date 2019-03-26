package me.yuanbin.kafka.task;

import me.yuanbin.common.config.AppConfigFactory;

import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractTask {

    private static final Logger logger = LoggerFactory.getLogger(AbstractTask.class);

    private static final String SOURCE_TOPICS = "source.topics";
    private static final String SINK_TOPICS = "sink.topics";
    protected static final String KEY_FILTER_REGEX = "key.filter_regex";
    protected static final String VALUE_FILTER_REGEX = "value.filter_regex";

    private static final Config appConfig = AppConfigFactory.load();
    protected final Config taskConfig = appConfig.getConfig("task." + this.getClass().getSimpleName());
    protected final List<String> sourceTopics = getStringList(taskConfig, SOURCE_TOPICS);
    protected final Config sinkTopicsConfig = taskConfig.getConfig(SINK_TOPICS);

    /**
     * get string list from given path
     * @param config TypesafeConfig instance
     * @param path relative path in config
     * @return list of string
     */
    protected List<String> getStringList(Config config, String path) {
        List<String> sourceTopics = ImmutableList.of();
        if (taskConfig.hasPath(path)) {
            try {
                sourceTopics = ImmutableList.copyOf(config.getStringList(path));
            } catch (ConfigException.WrongType ex) {
                logger.warn("path {} is not list, get string instead...", path);
                sourceTopics = ImmutableList.of(config.getString(path));
            }
        }
        return sourceTopics;
    }

    /**
     * get regex string from given path,
     * .* by default if empty, concatenate list of strings
     * @param config TypesafeConfig instance
     * @param path relative path in config
     * @return regex string
     */
    protected String getRegex(Config config, String path) {
        List<String> regexList = getStringList(config, path);
        if (regexList.size() == 0) {
            return ".*";
        } else if (regexList.size() == 1) {
            return regexList.get(0);
        } else {
            return regexList.stream().collect(Collectors.joining("|", "(", ")"));
        }
    }

    public abstract void build(StreamsBuilder builder);
}
