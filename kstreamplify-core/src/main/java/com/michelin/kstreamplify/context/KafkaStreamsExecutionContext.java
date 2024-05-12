package com.michelin.kstreamplify.context;

import static com.michelin.kstreamplify.property.PropertiesUtils.PROPERTY_SEPARATOR;
import static com.michelin.kstreamplify.serde.TopicWithSerde.SELF;
import static com.michelin.kstreamplify.topic.TopicUtils.PREFIX_PROPERTY_NAME;

import java.util.Map;
import java.util.Properties;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.StreamsConfig;

/**
 * The class to represent the context of the KStream.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsExecutionContext {

    /**
     * The DLQ topic name.
     */
    @Getter
    @Setter
    private static String dlqTopicName;

    /**
     * The Serdes config Map.
     */
    @Getter
    @Setter
    private static Map<String, String> serdeConfig;

    /**
     * The properties of the stream execution context.
     */
    @Getter
    @Setter
    private static Properties properties;

    /**
     * <p>The prefix that will be applied to the application.id if provided.</p>
     * <p>it needs to be defined like this:</p>
     * <pre>{@code
     * kafka:
     *   properties:
     *     prefix:
     *       self: "myNamespacePrefix."
     * }</pre>
     */
    @Getter
    private static String prefix;

    /**
     * Register KStream properties.
     *
     * @param properties The Kafka Streams properties
     */
    public static void registerProperties(Properties properties) {
        if (properties == null) {
            return;
        }

        prefix = properties.getProperty(PREFIX_PROPERTY_NAME + PROPERTY_SEPARATOR + SELF, "");
        if (StringUtils.isNotBlank(prefix)
            && properties.containsKey(StreamsConfig.APPLICATION_ID_CONFIG)) {
            properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,
                prefix.concat(properties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG)));
        }

        KafkaStreamsExecutionContext.properties = properties;
        StringBuilder stringBuilderProperties = new StringBuilder("Kafka Stream properties:\n");
        properties.forEach(
            (key, value) -> stringBuilderProperties.append("\t").append(key).append(" = ")
                .append(value).append("\n"));
        log.info(stringBuilderProperties.toString());
    }
}
