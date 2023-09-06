package com.michelin.kstreamplify.initializer;

import static com.michelin.kstreamplify.constants.InitializerConstants.SERVER_PORT_PROPERTY;

import com.michelin.kstreamplify.constants.InitializerConstants;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.properties.PropertiesUtils;
import com.michelin.kstreamplify.rest.DefaultProbeController;
import java.util.HashMap;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.state.HostInfo;

/**
 * The Kafka Streams initializer class.
 */
@Slf4j
@Getter
public class KafkaStreamsInitializer {
    /**
     * The Kafka Streams instance.
     */
    private KafkaStreams kafkaStreams;

    /**
     * The Kafka Streams starter.
     */
    private KafkaStreamsStarter kafkaStreamsStarter;

    /**
     * The topology.
     */
    private Topology topology;

    /**
     * The Kafka properties.
     */
    protected Properties kafkaProperties;

    /**
     * The application properties.
     */
    protected Properties properties;

    /**
     * The DLQ topic.
     */
    private String dlq;

    /**
     * The host info.
     */
    private HostInfo hostInfo;

    /**
     * The server port.
     */
    protected int serverPort;

    /**
     * Init the Kafka Streams.
     *
     * @param streamsStarter The Kafka Streams starter
     */
    public void init(KafkaStreamsStarter streamsStarter) {
        kafkaStreamsStarter = streamsStarter;

        initProperties();

        initSerdesConfig();

        initDlq();

        initHostInfo();

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        kafkaStreamsStarter.topology(streamsBuilder);

        topology = streamsBuilder.build();
        log.info("Topology description:\n {}", topology.describe());

        kafkaStreams = new KafkaStreams(topology, KafkaStreamsExecutionContext.getProperties());

        kafkaStreamsStarter.onStart(kafkaStreams);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        kafkaStreams.setUncaughtExceptionHandler(this::onStreamsUncaughtException);

        kafkaStreams.setStateListener(this::onStateChange);

        kafkaStreams.start();
        initHttpServer();
    }

    /**
     * Init the Kafka Streams execution context.
     */
    private void initSerdesConfig() {
        KafkaStreamsExecutionContext.setSerdesConfig(
            kafkaProperties.entrySet().stream().collect(
                Collectors.toMap(
                    e -> String.valueOf(e.getKey()),
                    e -> String.valueOf(e.getValue()),
                    (prev, next) -> next, HashMap::new
                ))
        );
    }

    /**
     * Init the Kafka Streams default DLQ.
     */
    private void initDlq() {
        dlq = kafkaStreamsStarter.dlqTopic();
        KafkaStreamsExecutionContext.setDlqTopicName(dlq);
    }

    /**
     * Init the host information.
     */
    private void initHostInfo() {
        String ipEnvVarName =
            (String) kafkaProperties.get(InitializerConstants.IP_SYSTEM_VARIABLE_PROPERTY);
        if (StringUtils.isBlank(ipEnvVarName)) {
            ipEnvVarName = InitializerConstants.IP_SYSTEM_VARIABLE_DEFAULT;
        }
        String myIp = System.getenv(ipEnvVarName);
        String host = StringUtils.isNotBlank(myIp) ? myIp : InitializerConstants.LOCALHOST;

        hostInfo = new HostInfo(host, serverPort);

        log.info("The Kafka Streams \"{}\" is running on {}:{}",
            KafkaStreamsExecutionContext.getProperties()
                .getProperty(StreamsConfig.APPLICATION_ID_CONFIG), hostInfo.host(),
            hostInfo.port());

        KafkaStreamsExecutionContext.getProperties().put(StreamsConfig.APPLICATION_SERVER_CONFIG,
            String.format("%s:%s", hostInfo.host(), hostInfo.port()));
    }

    /**
     * Init the HTTP server.
     */
    protected void initHttpServer() {
        new DefaultProbeController(this);
    }

    /**
     * Init all properties.
     */
    protected void initProperties() {
        properties = PropertiesUtils.loadProperties();

        serverPort = (Integer) properties.get(SERVER_PORT_PROPERTY);

        kafkaProperties = PropertiesUtils.loadKafkaProperties(properties);

        KafkaStreamsExecutionContext.registerProperties(kafkaProperties);
    }

    /**
     * Default uncaught exception handler.
     *
     * @param exception The exception
     * @return The execution
     */
    protected StreamsUncaughtExceptionHandler
        .StreamThreadExceptionResponse onStreamsUncaughtException(Throwable exception) {
        log.error("A not covered exception occurred in {} Kafka Streams. Shutting down...",
            kafkaProperties.get(StreamsConfig.APPLICATION_ID_CONFIG), exception);
        return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
    }

    /**
     * Default state change listener.
     *
     * @param newState The new state
     * @param oldState The old state
     */
    protected void onStateChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
        if (newState.equals(KafkaStreams.State.ERROR)) {
            log.error("The {} Kafka Streams is in error state...",
                kafkaProperties.get(StreamsConfig.APPLICATION_ID_CONFIG));
            System.exit(3);
        }
    }
}
