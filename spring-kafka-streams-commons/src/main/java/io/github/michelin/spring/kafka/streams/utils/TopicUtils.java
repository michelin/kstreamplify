package io.github.michelin.spring.kafka.streams.utils;

import io.github.michelin.spring.kafka.streams.context.KafkaStreamsExecutionContext;

/**
 * The topic utils class
 */
public class TopicUtils {
    private TopicUtils() { }

    /**
     * Prefix the given topic name with the configured prefix
     * @param topicName The topic name
     * @return The prefixed topic name
     */
    public static String prefix(String topicName) {
        if (KafkaStreamsExecutionContext.getPrefix() != null) {
            return KafkaStreamsExecutionContext.getPrefix() + topicName;
        }

        return topicName;
    }
}
