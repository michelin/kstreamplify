package com.michelin.kstreamplify.initializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.property.KafkaProperties;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ConfigurableApplicationContext;

@ExtendWith(MockitoExtension.class)
class SpringBootKafkaStreamsInitializerTest {
    @Mock
    private ConfigurableApplicationContext applicationContext;

    @Mock
    private KafkaStreamsStarter kafkaStreamsStarter;

    @Mock
    private KafkaProperties kafkaProperties;

    @InjectMocks
    private SpringBootKafkaStreamsInitializer initializer;

    @Test
    void shouldInitProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("prefix.self", "abc.");

        when(kafkaProperties.asProperties()).thenReturn(properties);

        initializer.initProperties();

        assertEquals(kafkaStreamsStarter, initializer.getKafkaStreamsStarter());
        assertNotNull(initializer.getKafkaProperties());
        assertEquals("abc.", KafkaStreamsExecutionContext.getPrefix());
        assertEquals("abc.appId", KafkaStreamsExecutionContext.getProperties()
            .get(StreamsConfig.APPLICATION_ID_CONFIG));
    }

    @Test
    void shouldCloseSpringBootContextOnUncaughtException() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("prefix.self", "abc.");

        when(kafkaProperties.asProperties()).thenReturn(properties);

        initializer.initProperties();
        StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse response = initializer
            .onStreamsUncaughtException(new RuntimeException("Unexpected test exception"));

        assertEquals(StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT, response);
        verify(applicationContext).close();
    }

    @Test
    void shouldCloseSpringBootContextOnChangeState() {
        initializer.onStateChange(KafkaStreams.State.ERROR, KafkaStreams.State.RUNNING);
        verify(applicationContext).close();
    }

    @Test
    void shouldNotCloseSpringBootContextOnChangeStateNotError() {
        initializer.onStateChange(KafkaStreams.State.REBALANCING, KafkaStreams.State.RUNNING);
        verify(applicationContext, never()).close();
    }
}
