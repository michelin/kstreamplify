package io.github.michelin.spring.kafka.streams.context;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;
import java.util.Properties;

/**
 * The class to represent the context of the KStream
 */
@Slf4j
public class KafkaStreamsExecutionContext {
    public static final String PREFIX_PROPERTY_NAME = "prefix";

    @Getter
    @Setter
    private static String dlqTopicName;

    @Getter
    @Setter
    private static Map<String, String> serdesConfig;

    @Getter
    private static Properties properties;

    @Getter
    private static String prefix;

    private KafkaStreamsExecutionContext() { }

    /**
     * Register KStream properties
     * @param properties
     */
    public static void registerProperties(Properties properties) {
        if (properties == null) {
            return;
        }

        prefix = properties.getProperty(PREFIX_PROPERTY_NAME,"");
        if (StringUtils.isNotBlank(prefix) && properties.containsKey(StreamsConfig.APPLICATION_ID_CONFIG)) {
            properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,
                    prefix.concat(properties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG)));
        }

        KafkaStreamsExecutionContext.properties = properties;
        log.info("Kafka Stream configuration: " + properties);
    }
}
