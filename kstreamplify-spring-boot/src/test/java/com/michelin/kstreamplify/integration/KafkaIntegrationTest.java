package com.michelin.kstreamplify.integration;

import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

@Slf4j
abstract class KafkaIntegrationTest {
    private static final Network NETWORK = Network.newNetwork();

    @Autowired
    protected KafkaStreamsInitializer initializer;

    @Autowired
    protected TestRestTemplate restTemplate;

    @Container
    static KafkaContainer broker = new KafkaContainer(DockerImageName
        .parse("confluentinc/cp-kafka:7.6.1"))
        //.withNetwork(NETWORK)
        //.withNetworkAliases("broker")
        .withKraft();

    /*@Container
    static GenericContainer<?> schemaRegistry = new GenericContainer<>(DockerImageName
        .parse("confluentinc/cp-schema-registry:7.6.1"))
        .dependsOn(broker)
        .withNetwork(NETWORK)
        .withNetworkAliases("schema-registry")
        .withExposedPorts(8081)
        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://broker:9092")
        .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));*/

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("kafka.properties." + BOOTSTRAP_SERVERS_CONFIG,
            broker::getBootstrapServers);
    }

    protected static void createTopics(String... topics) {
        var newTopics = Arrays.stream(topics)
            .map(topic -> new NewTopic(topic, 1, (short) 1))
            .toList();
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers()))) {
            admin.createTopics(newTopics);
        }
    }

    protected void waitingForKafkaStreamsToStart() throws InterruptedException {
        while (!initializer.getKafkaStreams().state().equals(KafkaStreams.State.RUNNING)) {
            log.info("Waiting for Kafka Streams to start...");
            Thread.sleep(2000);
        }
    }
}
