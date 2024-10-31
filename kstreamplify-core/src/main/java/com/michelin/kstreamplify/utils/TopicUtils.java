package com.michelin.kstreamplify.utils;

import static com.michelin.kstreamplify.property.PropertiesUtils.PROPERTY_SEPARATOR;
import static com.michelin.kstreamplify.topic.TopicUtils.PREFIX_PROPERTY_NAME;
import static com.michelin.kstreamplify.topic.TopicUtils.REMAP_PROPERTY_NAME;
import static com.michelin.kstreamplify.topic.TopicUtils.TOPIC_PROPERTY_NAME;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;

/**
 * The topic utils class.
 *
 * @deprecated Use {@link com.michelin.kstreamplify.topic.TopicUtils}.
 */
@Deprecated(since = "1.1.0")
public final class TopicUtils {
    private TopicUtils() {
    }

    /**
     * Prefix the given topic name with the configured prefix and applies the dynamic remap.
     * Prefix is retrieved at runtime from kafka.properties.prefix.[prefixPropertyKey]
     * <pre>{@code
     * kafka:
     *   properties:
     *     prefix:
     *       self: "myNamespacePrefix."
     * }</pre>
     * This allows interactions with multiple topics from different owners/namespaces
     * If not provided, prefixing will not occur.
     * <br/>
     * Dynamic remap is retrieved from the configuration like so:
     * <pre>{@code
     * kafka:
     *   properties:
     *     prefix:
     *       topic:
     *          myInitialTopicName: "myRemappedTopicName"
     * }</pre>
     * It can be applied to both input and output topics.
     *
     * @param topicName         The topicName that needs to be prefixed and remapped
     * @param prefixPropertyKey The prefixPropertyKey matching the configuration file
     * @return The prefixed and/or remapped topic.
     */
    public static String prefixAndDynamicRemap(String topicName, String prefixPropertyKey) {
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
        String prefix =
            properties.getProperty(PREFIX_PROPERTY_NAME + PROPERTY_SEPARATOR + prefixPropertyKey,
                "");
        return prefix.concat(resultTopicName);
    }
}
