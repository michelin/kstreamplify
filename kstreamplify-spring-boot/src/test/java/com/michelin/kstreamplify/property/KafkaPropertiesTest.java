package com.michelin.kstreamplify.property;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

class KafkaPropertiesTest {
    private final KafkaProperties kafkaProperties = new KafkaProperties();

    @Test
    void shouldLoadProperties() {
        Map<String, String> props = Map.of(APPLICATION_ID_CONFIG, "appId");

        kafkaProperties.setProperties(props);

        assertTrue(kafkaProperties.getProperties().containsKey(APPLICATION_ID_CONFIG));
        assertTrue(kafkaProperties.getProperties().containsValue("appId"));
        assertTrue(kafkaProperties.asProperties().containsKey(APPLICATION_ID_CONFIG));
        assertTrue(kafkaProperties.asProperties().containsValue("appId"));
    }
}
