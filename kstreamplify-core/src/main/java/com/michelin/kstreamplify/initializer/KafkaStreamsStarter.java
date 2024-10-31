package com.michelin.kstreamplify.initializer;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

/**
 * The Kafka Streams starter interface.
 */
public abstract class KafkaStreamsStarter {
    /**
     * Define the topology of the Kafka Streams.
     *
     * @param streamsBuilder The streams builder
     */
    public abstract void topology(StreamsBuilder streamsBuilder);

    /**
     * Define the dead letter queue (DLQ) topic.
     * If you don't want to use the DLQ topic, you can return {@link org.apache.commons.lang3.StringUtils#EMPTY}.
     *
     * @return The dead letter queue (DLQ) topic
     */
    public abstract String dlqTopic();

    /**
     * Define runnable code after the Kafka Streams startup.
     *
     * @param kafkaStreams The Kafka Streams instance
     */
    public void onStart(KafkaStreams kafkaStreams) {
    }

    /**
     * Register a custom uncaught exception handler.
     *
     * @return StreamsUncaughtExceptionHandler The custom uncaught exception handler
     */
    public StreamsUncaughtExceptionHandler uncaughtExceptionHandler() {
        return null;
    }
}
