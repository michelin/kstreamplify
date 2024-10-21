package com.michelin.kstreamplify.integration.container;

import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.property.PropertiesUtils;
import java.net.http.HttpClient;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.LagInfo;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base class for Kafka integration tests.
 */
@Slf4j
public abstract class KafkaIntegrationTest {
    protected static final String CONFLUENT_PLATFORM_VERSION = "7.7.0";
    protected static final Network NETWORK = Network.newNetwork();
    protected final HttpClient httpClient = HttpClient.newBuilder().build();
    protected final ObjectMapper objectMapper = new ObjectMapper();
    protected static KafkaStreamsInitializer initializer;

    @Container
    protected static ConfluentKafkaContainer broker = new ConfluentKafkaContainer(DockerImageName
        .parse("confluentinc/cp-kafka:" + CONFLUENT_PLATFORM_VERSION))
        .withNetwork(NETWORK)
        .withNetworkAliases("broker");

    @Container
    protected static GenericContainer<?> schemaRegistry = new GenericContainer<>(DockerImageName
        .parse("confluentinc/cp-schema-registry:" + CONFLUENT_PLATFORM_VERSION))
        .dependsOn(broker)
        .withNetwork(NETWORK)
        .withNetworkAliases("schema-registry")
        .withExposedPorts(8081)
        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "broker:9093")
        .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    protected static void createTopics(String bootstrapServers, TopicPartition... topicPartitions) {
        var newTopics = Arrays.stream(topicPartitions)
            .map(topicPartition -> new NewTopic(topicPartition.topic(), topicPartition.partition(), (short) 1))
            .toList();
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            admin.createTopics(newTopics);
        }
    }

    protected void waitingForKafkaStreamsToStart() throws InterruptedException {
        while (!initializer.getKafkaStreams().state().equals(KafkaStreams.State.RUNNING)) {
            log.info("Waiting for Kafka Streams to start...");
            Thread.sleep(2000); // NOSONAR
        }
    }

    protected void waitingForLocalStoreToReachOffset(Map<String, Map<Integer, Long>> topicPartitionOffset)
        throws InterruptedException {

        while (hasLag(topicPartitionOffset)) {
            log.info("Waiting for local stores {} to reach offsets", topicPartitionOffset.keySet().stream().toList());
            Thread.sleep(5000); // NOSONAR
        }
    }

    private boolean hasLag(Map<String, Map<Integer, Long>> topicPartitionOffset) {
        Map<String, Map<Integer, LagInfo>> currentLag = initializer.getKafkaStreams().allLocalStorePartitionLags();

        return !topicPartitionOffset.entrySet()
            .stream()
            .allMatch(topicPartitionOffsetEntry -> topicPartitionOffsetEntry.getValue().entrySet()
                .stream()
                .anyMatch(partitionOffsetEntry -> currentLag.get(topicPartitionOffsetEntry.getKey())
                    .get(partitionOffsetEntry.getKey())
                    .currentOffsetPosition() == partitionOffsetEntry.getValue()));
    }

    /**
     * Define a KafkaStreamsInitializer stub for testing.
     * This stub allows to override some properties of the application.properties file
     * or to set some properties dynamically from Testcontainers.
     */
    @AllArgsConstructor
    public static class KafkaStreamInitializerStub extends KafkaStreamsInitializer {
        private Integer newServerPort;
        private Map<String, String> additionalProperties;

        public KafkaStreamInitializerStub(Map<String, String> properties) {
            this.additionalProperties = properties;
        }

        /**
         * Override properties of the application.properties file.
         * Some properties are dynamically set from Testcontainers.
         */
        @Override
        protected void initProperties() {
            super.initProperties();

            if (newServerPort != null) {
                serverPort = newServerPort;
            }

            properties.putAll(additionalProperties);

            Properties convertedAdditionalProperties = new Properties();
            convertedAdditionalProperties.putAll(additionalProperties);
            kafkaProperties.putAll(PropertiesUtils.loadKafkaProperties(convertedAdditionalProperties));
            KafkaStreamsExecutionContext.registerProperties(kafkaProperties);
        }
    }
}
