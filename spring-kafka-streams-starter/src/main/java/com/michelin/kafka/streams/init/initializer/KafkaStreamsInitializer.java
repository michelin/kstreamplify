package com.michelin.kafka.streams.init.initializer;

import com.michelin.kafka.streams.init.properties.KafkaProperties;
import com.michelin.kafka.streams.starter.commons.context.KafkaStreamsExecutionContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

@Slf4j
@Import(KafkaProperties.class)
@ConditionalOnBean(KafkaStreamsStarter.class)
public class KafkaStreamsInitializer implements ApplicationRunner {
    @Autowired
    private ConfigurableApplicationContext applicationContext;

    @Autowired
    private KafkaStreamsStarter kafkaStreamsStarter;

    @Autowired
    private KafkaProperties kafkaProperties;

    @Getter
    private KafkaStreams kafkaStreams;

    @Getter
    private HostInfo hostInfo;

    @Value("${server.port:8080}")
    private int serverPort;

    @Override
    public void run(ApplicationArguments args) {
        initStreamExecutionContext();

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        kafkaStreamsStarter.topology(streamsBuilder);
        Topology topology = streamsBuilder.build();

        log.info("Description of the topology:\n {}", topology.describe());

        kafkaStreams = new KafkaStreams(topology, KafkaStreamsExecutionContext.getProperties());

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        kafkaStreams.setUncaughtExceptionHandler(exception -> {
            log.error("A not covered exception occurred in {} Kafka Streams. Shutting down...",
                    kafkaProperties.asProperties().get(StreamsConfig.APPLICATION_ID_CONFIG), exception);
            applicationContext.close();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState.equals(KafkaStreams.State.ERROR)) {
                log.error("The {} Kafka Streams is in error state...",
                        kafkaProperties.asProperties().get(StreamsConfig.APPLICATION_ID_CONFIG));

                applicationContext.close();
            }
        });

        kafkaStreams.start();
    }

    private void initStreamExecutionContext() {
        KafkaStreamsExecutionContext.setDlqTopicName(kafkaStreamsStarter.dlqTopic());
        KafkaStreamsExecutionContext.registerProperties(kafkaProperties.asProperties());
        KafkaStreamsExecutionContext.setSerdesConfig(kafkaProperties.getProperties());
        initHostInfo();
    }

    private void initHostInfo() {
        String host = org.springframework.util.StringUtils.hasText(System.getenv("MY_POD_IP")) ?
                System.getenv("MY_POD_IP") : "localhost";

        hostInfo = new HostInfo(host, serverPort);

        log.info("The Kafka Streams {} is running on {}", KafkaStreamsExecutionContext.getProperties()
                .getProperty(StreamsConfig.APPLICATION_ID_CONFIG), hostInfo);

        KafkaStreamsExecutionContext.getProperties().put(StreamsConfig.APPLICATION_SERVER_CONFIG,
                String.format("%s:%s", hostInfo.host(), hostInfo.port()));
    }

    @Bean
    public KafkaStreams kafkaStreams() {
        return kafkaStreams;
    }

    @Bean
    public HostInfo hostInfo() {
        return hostInfo;
    }
}
