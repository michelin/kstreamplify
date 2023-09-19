package com.michelin.kstreamplify.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsExecutionContextTest {

    @BeforeEach
    void setUp() {
        KafkaStreamsExecutionContext.setProperties(null);
    }

    @Test
    void shouldNotRegisterPropertiesWhenNull() {
        KafkaStreamsExecutionContext.registerProperties(null);
        assertNull(KafkaStreamsExecutionContext.getProperties());
    }

    @Test
    void shouldAddPrefixToAppId() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        properties.put("prefix.self", "abc.");

        KafkaStreamsExecutionContext.registerProperties(properties);

        assertEquals("abc.", KafkaStreamsExecutionContext.getPrefix());
        assertEquals("abc.appId", KafkaStreamsExecutionContext.getProperties()
            .get(StreamsConfig.APPLICATION_ID_CONFIG));
    }

    @Test
    void shouldNotAddPrefixToAppIdIfNoPrefix() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");

        KafkaStreamsExecutionContext.registerProperties(properties);

        assertEquals("", KafkaStreamsExecutionContext.getPrefix());
        assertEquals("appId", KafkaStreamsExecutionContext.getProperties()
            .get(StreamsConfig.APPLICATION_ID_CONFIG));
    }

    @Test
    void shouldNotAddPrefixToAppIdIfNotAppId() {
        Properties properties = new Properties();
        properties.put("prefix.self", "abc.");

        KafkaStreamsExecutionContext.registerProperties(properties);

        assertEquals("abc.", KafkaStreamsExecutionContext.getPrefix());
        assertNull(KafkaStreamsExecutionContext.getProperties().get(StreamsConfig.APPLICATION_ID_CONFIG));
    }
}
