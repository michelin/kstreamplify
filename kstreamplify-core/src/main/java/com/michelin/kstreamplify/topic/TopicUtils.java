package com.michelin.kstreamplify.topic;

import static com.michelin.kstreamplify.property.PropertiesUtils.PROPERTY_SEPARATOR;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import java.util.Properties;
import lombok.NoArgsConstructor;

/**
 * The topic utils class.
 */
@NoArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public final class TopicUtils {
    /**
     * The topic property name.
     */
    public static final String TOPIC_PROPERTY_NAME = "topic";

    /**
     * The prefix property name.
     */
    public static final String PREFIX_PROPERTY_NAME = "prefix";

    /**
     * The remap property name.
     */
    public static final String REMAP_PROPERTY_NAME = "remap";

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
     *     topic:
     *       remap:
     *          myInitialTopicName: "myRemappedTopicName"
     * }</pre>
     * <p>It can be applied to both input and output topics</p>
     *
     * @param topicName         The topicName that needs to be prefixed and remapped
     * @param prefixPropertyKey The prefixPropertyKey matching the configuration file
     * @return The prefixed and/or remapped topic.
     */
    public static String remapAndPrefix(String topicName, String prefixPropertyKey) {
        Properties properties = KafkaStreamsExecutionContext.getProperties();

        // Check for dynamic remap in properties
        String resultTopicName = properties.getProperty(TOPIC_PROPERTY_NAME
            + PROPERTY_SEPARATOR + REMAP_PROPERTY_NAME + PROPERTY_SEPARATOR + topicName, topicName);

        // Check if topic prefix property exists
        String prefix = properties.getProperty(PREFIX_PROPERTY_NAME + PROPERTY_SEPARATOR + prefixPropertyKey, "");
        return prefix.concat(resultTopicName);
    }
}
