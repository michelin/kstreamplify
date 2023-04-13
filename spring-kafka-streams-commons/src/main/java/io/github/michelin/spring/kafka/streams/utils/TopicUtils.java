package io.github.michelin.spring.kafka.streams.utils;

import io.github.michelin.spring.kafka.streams.context.KafkaStreamsExecutionContext;

public class TopicUtils {
    private TopicUtils() { }

    public static String prefix(String topicName) {
        if (KafkaStreamsExecutionContext.getPrefix() != null) {
            return KafkaStreamsExecutionContext.getPrefix() + topicName;
        }

        return topicName;
    }
}
