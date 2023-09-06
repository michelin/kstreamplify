package com.michelin.kstreamplify.initializer;

import org.apache.kafka.streams.KafkaStreams;
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
     * <p>Define the dead letter queue (DLQ) topic</p>
     * <p>If you don't want to use the DLQ topic, you can return {@link org.apache.commons.lang3.StringUtils#EMPTY}</p>
     *
     * @return The dead letter queue (DLQ) topic
     */
    String dlqTopic();

    /**
     * Define runnable code after the Kafka Streams startup
     * @param kafkaStreams The Kafka Streams instance
     */
    default void onStart(KafkaStreams kafkaStreams) { }
}
