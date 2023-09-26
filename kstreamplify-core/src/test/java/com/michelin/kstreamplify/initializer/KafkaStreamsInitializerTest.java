package com.michelin.kstreamplify.initializer;

import static com.michelin.kstreamplify.constants.InitializerConstants.SERVER_PORT_PROPERTY;
import static com.michelin.kstreamplify.constants.PropertyConstants.KAFKA_PROPERTIES_PREFIX;
import static com.michelin.kstreamplify.constants.PropertyConstants.PROPERTY_SEPARATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.properties.PropertiesUtils;
import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaStreamsInitializerTest {

    private final KafkaStreamsInitializer initializer = new KafkaStreamsInitializer();

    @Test
    void shouldInitProperties() {
        try (MockedStatic<PropertiesUtils> propertiesUtilsMockedStatic = mockStatic(PropertiesUtils.class)) {
            Properties properties = new Properties();
            properties.put(SERVER_PORT_PROPERTY, 8080);
            properties.put(KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + StreamsConfig.APPLICATION_ID_CONFIG, "appId");
            properties.put(KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + "prefix.self", "abc.");

            propertiesUtilsMockedStatic.when(PropertiesUtils::loadProperties)
                .thenReturn(properties);

            propertiesUtilsMockedStatic.when(() -> PropertiesUtils.loadKafkaProperties(any())).thenCallRealMethod();

            initializer.initProperties();

            assertNotNull(initializer.getProperties());
            assertEquals(8080, initializer.getServerPort());
            assertTrue(initializer.getKafkaProperties().containsKey(StreamsConfig.APPLICATION_ID_CONFIG));
            assertEquals("abc.", KafkaStreamsExecutionContext.getPrefix());
            assertEquals("abc.appId", KafkaStreamsExecutionContext.getProperties()
                .get(StreamsConfig.APPLICATION_ID_CONFIG));
        }
    }

    @Test
    void shouldShutdownClientOnUncaughtException() {
        try (MockedStatic<PropertiesUtils> propertiesUtilsMockedStatic = mockStatic(PropertiesUtils.class)) {
            Properties properties = new Properties();
            properties.put(SERVER_PORT_PROPERTY, 8080);
            properties.put(KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + StreamsConfig.APPLICATION_ID_CONFIG, "appId");

            propertiesUtilsMockedStatic.when(PropertiesUtils::loadProperties)
                .thenReturn(properties);

            propertiesUtilsMockedStatic.when(() -> PropertiesUtils.loadKafkaProperties(any()))
                .thenCallRealMethod();

            initializer.initProperties();

            StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse response = initializer
                .onStreamsUncaughtException(new RuntimeException("Test Exception"));

            assertEquals(StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT, response);
        }
    }
}
