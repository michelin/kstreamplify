package com.michelin.kstreamplify.initializer;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

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
     * <p>Define the dead letter queue (DLQ) topic</p>.
     * <p>If you don't want to use the DLQ topic, you can return {@link org.apache.commons.lang3.StringUtils#EMPTY}</p>
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
}
