package com.michelin.kstreamplify.initializer;

import com.michelin.kstreamplify.properties.KafkaProperties;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.ConfigurableApplicationContext;

import java.lang.reflect.Field;

import static org.mockito.Mockito.*;

class SpringKafkaStreamsInitializerTest {

    @Mock
    private ConfigurableApplicationContext applicationContext;

    @Mock
    private ApplicationArguments applicationArguments;

    @Mock
    private KafkaStreamsStarter kafkaStreamsStarter;

    private KafkaProperties kafkaProperties;

    private SpringKafkaStreamsInitializer initializer;

    @Mock
    private SpringKafkaStreamsInitializer initializerMocked;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
        kafkaProperties = new KafkaProperties();
        initializer = new SpringKafkaStreamsInitializer();
        setPrivateField(initializer, "applicationContext", applicationContext);
        setPrivateField(initializer, "springBootServerPort", 8080);
        setPrivateField(initializer, "springBootKafkaProperties", kafkaProperties);
        setPrivateField(initializer, "kafkaStreamsStarter", kafkaStreamsStarter);
    }

    @Test
    void testRun() {
        doCallRealMethod().when(initializerMocked).run(applicationArguments);
        initializerMocked.run(applicationArguments);
    }

    @Test
    void testInitProperties() {
        initializer.initProperties();
    }

    @Test
    void testOnStreamsUncaughtException() {
        Throwable exception = new RuntimeException("Test Exception");
        initializer.initProperties();
        kafkaProperties.getProperties();
        initializer.onStreamsUncaughtException(exception);
        verify(applicationContext).close();
    }

    @Test
    void testOnStateChangeErrorState() {
        KafkaStreams.State newState = KafkaStreams.State.ERROR;
        KafkaStreams.State oldState = KafkaStreams.State.RUNNING;
        initializer.onStateChange(newState, oldState);
        verify(applicationContext).close();
    }

    @Test
    void testOnStateChangeErrorStateWithoutContext() {
        setPrivateField(initializer, "applicationContext", null);
        KafkaStreams.State newState = KafkaStreams.State.ERROR;
        KafkaStreams.State oldState = KafkaStreams.State.RUNNING;
        initializer.onStateChange(newState, oldState);
    }

    @Test
    void testOnStateChangeNonErrorState() {
        KafkaStreams.State newState = KafkaStreams.State.RUNNING;
        KafkaStreams.State oldState = KafkaStreams.State.REBALANCING;
        initializer.onStateChange(newState, oldState);
        verify(applicationContext, never()).close();
    }

    @Test
    void testInitHttpServer() {
        initializer.initHttpServer();
    }

    private void setPrivateField(Object object, String fieldName, Object value) {
        try {
            Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
