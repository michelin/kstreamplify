package io.github.michelin.spring.kafka.streams.initializer;

import io.github.michelin.spring.kafka.streams.properties.KafkaProperties;
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

import java.io.IOException;

/**
 * The Kafka Streams initializer class
 */
@Slf4j
@Component
@ConditionalOnBean(KafkaStreamsStarter.class)
public class SpringKafkaStreamsInitializer extends KafkaStreamsInitializer implements ApplicationRunner {
    /**
     * The application context
     */
    @Autowired
    private ConfigurableApplicationContext springAppplicationContext;

    @Value("${server.port:8080}")
    protected int serverPort; //todo check if this works otherwise add additional spring field then assign to parent field in init method

    @Autowired
    KafkaProperties springProperties;

    /**
     * The Kafka Streams starter
     */
    @Autowired
    private KafkaStreamsStarter kafkaStreamsStarter;


    /**
     * The run method
     *
     * @param args the program arguments
     */
    @Override
    public void run(ApplicationArguments args) throws IOException {
        init(kafkaStreamsStarter);
    }

    @Override
    protected void initHttpServer() {
    }

    @Override
    protected void initProperties() {
    }

    @Override
    protected StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse onStreamsUncaughtException(Throwable exception) {
        closeSpringContextIfSet();
        return super.onStreamsUncaughtException(exception);
    }

    @Override
    protected void onStateChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
        if (newState.equals(KafkaStreams.State.ERROR)) {
            closeSpringContextIfSet();
        }
    }

    private void closeSpringContextIfSet() {
        if (springAppplicationContext != null) {
            springAppplicationContext.close();
        } else {
            log.warn("No Spring Context set");
        }
    }
}
