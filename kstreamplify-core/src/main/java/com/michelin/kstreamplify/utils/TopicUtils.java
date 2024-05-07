package com.michelin.kstreamplify.utils;

import static com.michelin.kstreamplify.property.PropertiesUtils.PROPERTY_SEPARATOR;
import static com.michelin.kstreamplify.util.TopicUtils.PREFIX_PROPERTY_NAME;
import static com.michelin.kstreamplify.util.TopicUtils.REMAP_PROPERTY_NAME;
import static com.michelin.kstreamplify.util.TopicUtils.TOPIC_PROPERTY_NAME;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;

/**
 * The topic utils class.
 *
 * @deprecated Use {@link com.michelin.kstreamplify.util.TopicUtils}.
 */
@Deprecated(forRemoval = true, since = "1.0.3")
public final class TopicUtils {
    private TopicUtils() {
    }

    /**
     * <p>Prefix the given topic name with the configured prefix and applies the dynamic remap.</p>
     * <p>Prefix is retrieved at runtime from kafka.properties.prefix.[prefixPropertyKey]</p>
     * <pre>{@code
     * kafka:
     *   properties:
     *     prefix:
     *       self: "myNamespacePrefix."
     * }</pre>
     * <p>This allows interactions with multiple topics from different owners/namespaces</p>
     * <p>If not provided, prefixing will not occur.</p>
     * <br/>
     * <p>Dynamic remap is retrieved from the configuration like so:</p>
     * <pre>{@code
     * kafka:
     *   properties:
     *     prefix:
     *       topic:
     *          myInitialTopicName: "myRemappedTopicName"
     * }</pre>
     * <p>It can be applied to both input and output topics</p>
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
