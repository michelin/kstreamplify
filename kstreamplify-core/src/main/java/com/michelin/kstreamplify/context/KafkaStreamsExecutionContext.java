package com.michelin.kstreamplify.context;

import com.michelin.kstreamplify.constants.PropertyConstants;
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

    private KafkaStreamsExecutionContext() {
    }

    /**
     * Register KStream properties
     *
     * @param properties The Kafka Streams properties
     */
    public static void registerProperties(Properties properties) {
        if (properties == null) {
            return;
        }

        prefix = properties.getProperty(PropertyConstants.PREFIX_PROPERTY_NAME, "");
        if (StringUtils.isNotBlank(prefix) && properties.containsKey(StreamsConfig.APPLICATION_ID_CONFIG)) {
            properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,
                    prefix.concat(properties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG)));
        }

        KafkaStreamsExecutionContext.properties = properties;
        StringBuilder stringBuilderProperties = new StringBuilder("Kafka Stream properties:\n");
        properties.forEach((key, value) -> stringBuilderProperties.append("\t").append(key).append(" = ").append(value).append("\n"));
        log.info(stringBuilderProperties.toString());
    }
}
