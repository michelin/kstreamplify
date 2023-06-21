package io.github.michelin.spring.kafka.streams.initializer;

import io.github.michelin.spring.kafka.streams.context.KafkaStreamsExecutionContext;
import io.github.michelin.spring.kafka.streams.properties.EnvProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.state.HostInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.stream.Collectors;


/**
 * The Kafka Streams initializer class
 */
@Slf4j
public class KafkaStreamsInitializer {

    /**
     * The Kafka Streams instance
     */
    @Getter
    private KafkaStreams kafkaStreams;

    @Getter
    private KafkaStreamsStarter kafkaStreamsStarter;

    /**
     * The topology
     */
    @Getter
    private Topology topology;

    private EnvProperties envProperties;

    @Getter
    private String dlq;
    /**
     * The host info
     */
    @Getter
    private HostInfo hostInfo;

    private int serverPort;

    public void init(KafkaStreamsStarter kafkaStreamsStarter) throws IOException {


        this.kafkaStreamsStarter = kafkaStreamsStarter;

        this.envProperties = new EnvProperties();

        this.dlq = this.kafkaStreamsStarter.dlqTopic();

        this.initStreamExecutionContext();

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        this.kafkaStreamsStarter.topology(streamsBuilder);

        this.topology = streamsBuilder.build();
        log.info("TOPOLOGY :"+ topology.describe());

        this.kafkaStreams = new KafkaStreams(topology, KafkaStreamsExecutionContext.getProperties());

        this.kafkaStreamsStarter.onStart();
        // On JVm shutdown, shutdown stream
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        this.kafkaStreams.setUncaughtExceptionHandler(exception -> {
            log.error("A not covered exception occurred in {} Kafka Streams. Shutting down...",
                    envProperties.getKafkaProperties().get(StreamsConfig.APPLICATION_ID_CONFIG), exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        this.kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState.equals(KafkaStreams.State.ERROR)) {
                log.error("The {} Kafka Streams is in error state...",
                        envProperties.getKafkaProperties().get(StreamsConfig.APPLICATION_ID_CONFIG));
                System.exit(3);
            }
        });

        this.kafkaStreams.start();


    }


    /**
     * Init the Kafka Streams execution context
     */
    private void initStreamExecutionContext() {


        envProperties.loadProperties();

        serverPort = (int) this.envProperties.getProperties().get("server.port");

        KafkaStreamsExecutionContext.registerProperties(this.envProperties.getKafkaProperties());
        KafkaStreamsExecutionContext.setDlqTopicName(dlq);

        KafkaStreamsExecutionContext.setSerdesConfig(
                this.envProperties.getKafkaProperties().entrySet().stream().collect(
                        Collectors.toMap(
                                e -> String.valueOf(e.getKey()),
                                e -> String.valueOf(e.getValue()),
                                (prev, next) -> next, HashMap::new
                        ))
        );
        initHostInfo();
    }

    /**
     * Init the host information
     */
    private void initHostInfo() {
        String host = StringUtils.isNotBlank(System.getenv("MY_POD_IP")) ?
                System.getenv("MY_POD_IP") : "localhost";

        hostInfo = new HostInfo(host, serverPort);

        log.info("The Kafka Streams \"{}\" is running on {}:{}", KafkaStreamsExecutionContext.getProperties()
                .getProperty(StreamsConfig.APPLICATION_ID_CONFIG), hostInfo.host(), hostInfo.port());

        KafkaStreamsExecutionContext.getProperties().put(StreamsConfig.APPLICATION_SERVER_CONFIG,
                String.format("%s:%s", hostInfo.host(), hostInfo.port()));
    }
}
