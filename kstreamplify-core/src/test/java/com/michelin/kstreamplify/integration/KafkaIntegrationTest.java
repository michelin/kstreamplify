package com.michelin.kstreamplify.integration;

import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import java.net.http.HttpClient;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public abstract class KafkaIntegrationTest {
    protected final KafkaStreamsInitializer initializer = new KafkaStreamInitializerStub();
    protected final HttpClient httpClient = HttpClient.newBuilder().build();

    @Container
    static KafkaContainer broker = new KafkaContainer(DockerImageName
        .parse("confluentinc/cp-kafka:7.6.1"))
        .withNetworkAliases("broker")
        .withKraft();

    protected static void createTopics(String... topics) {
        var newTopics = Arrays.stream(topics)
            .map(topic -> new NewTopic(topic, 1, (short) 1))
            .toList();
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers()))) {
            admin.createTopics(newTopics);
        }
    }

    protected void waitingForKafkaStreamsToRun() throws InterruptedException {
        while (!initializer.getKafkaStreams().state().equals(KafkaStreams.State.RUNNING)) {
            log.info("Waiting for Kafka Streams to start...");
            Thread.sleep(2000);
        }
    }

    static class KafkaStreamInitializerStub extends KafkaStreamsInitializer {
        @Override
        protected void initProperties() {
            super.initProperties();
            KafkaStreamsExecutionContext.getProperties()
                .setProperty(BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers());
        }
    }
}
