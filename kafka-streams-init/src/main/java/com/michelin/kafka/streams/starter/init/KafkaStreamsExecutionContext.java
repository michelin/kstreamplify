package com.michelin.kafka.streams.starter.init;

import io.micrometer.common.util.StringUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;
import java.util.Properties;

@Slf4j
public class KafkaStreamsExecutionContext {
    private static final Integer DEFAULT_PARTITION_NUMBER = 6;

    @Getter
    @Setter
    private static String dlqTopicName;

    @Getter
    @Setter
    private static Map<String, String> serdesConfig;

    @Getter
    private static Properties properties;
    @Setter
    private static Integer numPartition = null;

    public static final String PREFIX = "prefix";


    private KafkaStreamsExecutionContext() { }

    public static void registerProperties(Properties properties) {
        if (properties == null) {
            return;
        }
        String prefix = properties.getProperty(PREFIX,"");
        if (StringUtils.isNotBlank(prefix) && properties.containsKey(StreamsConfig.APPLICATION_ID_CONFIG)) {
            properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,
                    prefix.concat(properties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG)));
        }

        KafkaStreamsExecutionContext.properties = properties;
        log.info("Stream configuration: " + properties);
    }

    public static Integer getNumberPartition() {
        return KafkaStreamsExecutionContext.numPartition == null ? DEFAULT_PARTITION_NUMBER : numPartition;
    }

    protected static void reset() {
        log.warn("dlqTopicName reset to null");
        dlqTopicName = null;
        log.warn("properties reset to null");
        properties = null;
        log.warn("serdesConfig reset to null");
        serdesConfig = null;
    }
}
