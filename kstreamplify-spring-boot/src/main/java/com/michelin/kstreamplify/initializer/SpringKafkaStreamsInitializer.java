package com.michelin.kstreamplify.initializer;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.properties.KafkaProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

/**
 * The Kafka Streams initializer class.
 */
@Slf4j
@Component
@ConditionalOnBean(KafkaStreamsStarter.class)
public class SpringKafkaStreamsInitializer extends KafkaStreamsInitializer
    implements ApplicationRunner {
    /**
     * The application context.
     */
    @Autowired
    private ConfigurableApplicationContext applicationContext;

    /**
     * The server port.
     */
    @Value("${server.port:8080}")
    private int springBootServerPort;

    /**
     * The Kafka properties.
     */
    @Autowired
    private KafkaProperties springBootKafkaProperties;

    /**
     * The Kafka Streams starter.
     */
    @Autowired
    private KafkaStreamsStarter kafkaStreamsStarter;

    /**
     * Run method.
     *
     * @param args the program arguments
     */
    @Override
    public void run(ApplicationArguments args) {
        init(kafkaStreamsStarter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void initHttpServer() {
        // Nothing to do here, Spring Boot is running its own HTTP server
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void initProperties() {
        serverPort = springBootServerPort;
        kafkaProperties = springBootKafkaProperties.asProperties();
        KafkaStreamsExecutionContext.registerProperties(kafkaProperties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse onStreamsUncaughtException(
        Throwable exception) {
        closeApplicationContext();
        return super.onStreamsUncaughtException(exception);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void onStateChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
        if (newState.equals(KafkaStreams.State.ERROR)) {
            closeApplicationContext();
        }
    }

    /**
     * Close the application context.
     */
    private void closeApplicationContext() {
        if (applicationContext != null) {
            applicationContext.close();
        } else {
            log.warn("No Spring context set");
        }
    }
}
