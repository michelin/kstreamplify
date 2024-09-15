package com.michelin.kstreamplify.property;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class PropertiesUtilsTest {

    @Test
    void shouldLoadProperties() {
        Properties properties = PropertiesUtils.loadProperties();

        assertTrue(properties.containsKey("server.port"));
        assertTrue(properties.containsValue(8080));

        assertTrue(properties.containsKey("kafka.properties." + APPLICATION_ID_CONFIG));
        assertTrue(properties.containsValue("appId"));
    }

    @Test
    void shouldLoadKafkaProperties() {
        Properties properties = PropertiesUtils.loadKafkaProperties(PropertiesUtils.loadProperties());

        assertTrue(properties.containsKey(APPLICATION_ID_CONFIG));
        assertTrue(properties.containsValue("appId"));
    }
}
