package com.michelin.kafka.streams.starter.commons.utils;

import com.michelin.kafka.streams.starter.commons.context.KafkaStreamsExecutionContext;

public class TopicUtils {
    private TopicUtils() { }

    public static String prefix(String topicName) {
        if (KafkaStreamsExecutionContext.getPrefix() != null) {
            return KafkaStreamsExecutionContext.getPrefix() + topicName;
        }

        return topicName;
    }
}
