package me.yuanbin.kafka.task;

import org.apache.kafka.streams.StreamsBuilder;

public interface ETLTask {

    /**
     * Kafka Stream ETL task builder
     * @param builder StreamsBuilder instance, created by StreamTasksFactory
     */
    void build(StreamsBuilder builder);
}
