package io.github.michelin.kstreamplify.initializer;

import io.github.michelin.kstreamplify.constants.InitializerConstants;
import io.github.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import io.github.michelin.kstreamplify.properties.PropertiesUtils;
import io.github.michelin.kstreamplify.rest.DefaultProbeController;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.state.HostInfo;

import java.util.HashMap;
import java.util.Properties;
import java.util.stream.Collectors;

import static io.github.michelin.kstreamplify.constants.InitializerConstants.SERVER_PORT_PROPERTY;

/**
 * The Kafka Streams initializer class
 */
@Slf4j
@Getter
public class KafkaStreamsInitializer {

    /**
     * The Kafka Streams instance
     */
    private KafkaStreams kafkaStreams;

    private KafkaStreamsStarter kafkaStreamsStarter;

    /**
     * The topology
     */
    private Topology topology;

    protected Properties kafkaProperties;

    protected Properties properties;

    private String dlq;
    /**
     * The host info
     */
    private HostInfo hostInfo;

    protected int serverPort;

    public void init(KafkaStreamsStarter kStreamsStarter) {

        kafkaStreamsStarter = kStreamsStarter;

        dlq = kafkaStreamsStarter.dlqTopic();

        initProperties();

        initStreamExecutionContext();

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        kafkaStreamsStarter.topology(streamsBuilder);

        topology = streamsBuilder.build();
        log.info("TOPOLOGY :" + topology.describe());

        kafkaStreams = new KafkaStreams(topology, KafkaStreamsExecutionContext.getProperties());

        kafkaStreamsStarter.onStart();
        // On JVm shutdown, shutdown stream
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        kafkaStreams.setUncaughtExceptionHandler(this::onStreamsUncaughtException);

        kafkaStreams.setStateListener(this::onStateChange);

        kafkaStreams.start();
        initHttpServer();
    }


    /**
     * Init the Kafka Streams execution context
     */
    private void initStreamExecutionContext() {
        KafkaStreamsExecutionContext.registerProperties(kafkaProperties);
        KafkaStreamsExecutionContext.setDlqTopicName(dlq);

        KafkaStreamsExecutionContext.setSerdesConfig(
                kafkaProperties.entrySet().stream().collect(
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
        String ipEnvVarName = (String) kafkaProperties.get(InitializerConstants.IP_SYSTEM_VARIABLE_PROPERTY);
        if (StringUtils.isBlank(ipEnvVarName)) {
            ipEnvVarName = InitializerConstants.IP_SYSTEM_VARIABLE_DEFAULT;
        }
        String myIP = System.getenv(ipEnvVarName);
        String host = StringUtils.isNotBlank(myIP) ? myIP : InitializerConstants.LOCALHOST;

        hostInfo = new HostInfo(host, serverPort);

        log.info("The Kafka Streams \"{}\" is running on {}:{}", KafkaStreamsExecutionContext.getProperties()
                .getProperty(StreamsConfig.APPLICATION_ID_CONFIG), hostInfo.host(), hostInfo.port());

        KafkaStreamsExecutionContext.getProperties().put(StreamsConfig.APPLICATION_SERVER_CONFIG,
                String.format("%s:%s", hostInfo.host(), hostInfo.port()));
    }

    protected void initHttpServer() {
        new DefaultProbeController(this);
    }

    protected void initProperties() {

        properties = PropertiesUtils.loadProperties();

        serverPort = (Integer) properties.get(SERVER_PORT_PROPERTY);;

        // DefaultBehavior load from file
        kafkaProperties = PropertiesUtils.loadKafkaProperties(properties);
    }


    protected StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse onStreamsUncaughtException(Throwable exception) {
        log.error("A not covered exception occurred in {} Kafka Streams. Shutting down...",
                kafkaProperties.get(StreamsConfig.APPLICATION_ID_CONFIG), exception);
        return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
    }

    protected void onStateChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
        if (newState.equals(KafkaStreams.State.ERROR)) {
            log.error("The {} Kafka Streams is in error state...",
                    kafkaProperties.get(StreamsConfig.APPLICATION_ID_CONFIG));
            System.exit(3);
        }
    }
}
