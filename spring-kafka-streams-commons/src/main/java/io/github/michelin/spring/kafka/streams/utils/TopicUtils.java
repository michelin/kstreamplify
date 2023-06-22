package io.github.michelin.spring.kafka.streams.utils;

import io.github.michelin.spring.kafka.streams.context.KafkaStreamsExecutionContext;

import java.util.Properties;

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

    /**
     * Prefix the given topic name with the configured prefix
     * @param topicName The topic name
     * @return The prefixed topic name
     */
    public static String prefixAndDynamicRemap(String topicName) {
        var properties = KafkaStreamsExecutionContext.getProperties();
        var finalTopicName = topicName;
//        if (properties != null) {
//            String remap_topic = properties.getProperty(TOPIC_REMAP + PROPERTIES_SEPRATOR + topicName, null);
//            if (null != remap_topic) {
//                finalTopicName = remap_topic;
//            }
//            // Looking for prefix to use in oom_topic.prefix.APP_NAME
//            String prefix_topic = properties.getProperty(TOPIC_PREFIX + PROPERTIES_SEPRATOR + appName, "");
//            finalTopicName = prefix_topic.concat(finalTopicName);
//        }
        return finalTopicName;
    }
    
    
}
