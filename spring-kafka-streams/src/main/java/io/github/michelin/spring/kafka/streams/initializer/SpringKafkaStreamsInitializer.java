package io.github.michelin.spring.kafka.streams.initializer;

import io.github.michelin.spring.kafka.streams.properties.KafkaProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Import;
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
     * The Kafka Streams starter
     */
    @Autowired
    private KafkaStreamsStarter kafkaStreamsStarter;


    /**
     * The run method
     * @param args the program arguments
     */
    @Override
    public void run(ApplicationArguments args) throws IOException {

        init(kafkaStreamsStarter);


    }



}
