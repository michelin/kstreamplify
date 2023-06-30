package io.github.michelin.kstreamplify.utils;

import io.github.michelin.kstreamplify.context.KafkaStreamsExecutionContext;

import static io.github.michelin.kstreamplify.constants.PropertyConstants.*;

/**
 * The topic utils class
 */
public final class TopicUtils {
    private TopicUtils() {
    }

    /**
     * Prefix the given topic name with the configured prefix and applies the dynamic remap.<br/>
     * Prefix can be inferred from kafka.properties.topic.prefix.[appName]<br/>
     * If not provided, prefixing will occur with the kafka.properties.prefix property.<br/><br/>
     * If you are using {@link TopicWithSerde}, the appName provided in the property file needs to match the one provided in the topic-defining static method
     *
     * @param topicName The topicName that needs to be prefixed and remapped
     * @param appName The appName matching the one matching the configuration file
     * @return The prefixed and/or remapped topic.
     */
    public static String prefixAndDynamicRemap(String topicName, String appName) {
        var properties = KafkaStreamsExecutionContext.getProperties();

        // Check for dynamic remap in properties
        String resultTopicName = properties.getProperty(
                TOPIC_PROPERTY_NAME
                        + PROPERTY_SEPARATOR
                        + REMAP_PROPERTY_NAME
                        + PROPERTY_SEPARATOR
                        + topicName,
                topicName);

        // check if topic prefix property exists
        String prefix = properties.getProperty(TOPIC_PROPERTY_NAME + PROPERTY_SEPARATOR + PREFIX_PROPERTY_NAME + PROPERTY_SEPARATOR + appName);
        // If it doesn't, use the prefix defined at the root of kafka.properties
        if (prefix == null) {
            prefix = properties.getProperty(TOPIC_PROPERTY_NAME + PROPERTY_SEPARATOR + PREFIX_PROPERTY_NAME, "");
        }
        return prefix.concat(resultTopicName);
    }


}
