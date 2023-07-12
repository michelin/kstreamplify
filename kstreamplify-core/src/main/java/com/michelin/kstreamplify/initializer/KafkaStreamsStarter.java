package com.michelin.kstreamplify.initializer;

import org.apache.kafka.streams.StreamsBuilder;

import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * The Kafka Streams starter interface
 */
public interface KafkaStreamsStarter {
    /**
     * Define the topology of the Kafka Streams
     * @param streamsBuilder The streams builder
     */
    void topology(StreamsBuilder streamsBuilder);

    /**
     * Define the dead letter queue (DLQ) topic
     * @return The dead letter queue (DLQ) topic
     */
    default String dlqTopic() { return EMPTY; }

    /**
     * Define runnable code after the Kafka Streams startup
     */
    default void onStart() { }
}
