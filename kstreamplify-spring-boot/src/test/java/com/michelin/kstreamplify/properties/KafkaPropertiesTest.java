package com.michelin.kstreamplify.properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

class KafkaPropertiesTest {

    private final KafkaProperties kafkaProperties = new KafkaProperties();

    @Test
    void shouldLoadProperties() {
        Map<String, String> props = Map.of(StreamsConfig.APPLICATION_ID_CONFIG, "appId");

        kafkaProperties.setProperties(props);

        assertTrue(kafkaProperties.getProperties().containsKey("application.id"));
        assertTrue(kafkaProperties.getProperties().containsValue("appId"));
        assertTrue(kafkaProperties.asProperties().containsKey("application.id"));
        assertTrue(kafkaProperties.asProperties().containsValue("appId"));
    }
}
