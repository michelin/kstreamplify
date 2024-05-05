package com.michelin.kstreamplify.integration;

import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
class KafkaStreamsInitializerIntegrationTest {
    private final KafkaStreamsInitializer initializer = new KafkaStreamInitializerImpl();

    private final HttpClient httpClient = HttpClient.newBuilder().build();

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName
        .parse("confluentinc/cp-kafka:7.4.0"))
        .withKraft();

    @BeforeAll
    static void setUp() {
        createTopics("inputTopic", "outputTopic");
    }

    @Test
    void shouldInitAndRun() throws InterruptedException, IOException {
        initializer.init(new KafkaStreamsStarterImpl());

        waitingForKafkaStreamsToRun();

        assertEquals(KafkaStreams.State.RUNNING, initializer.getKafkaStreams().state());

        List<StreamsMetadata> streamsMetadata =
            new ArrayList<>(initializer.getKafkaStreams().metadataForAllStreamsClients());

        // Assert Kafka Streams initialization
        assertEquals("localhost", streamsMetadata.get(0).hostInfo().host());
        assertEquals(8080, streamsMetadata.get(0).hostInfo().port());
        assertTrue(streamsMetadata.get(0).stateStoreNames().isEmpty());

        List<TopicPartition> topicPartitions = streamsMetadata.get(0).topicPartitions().stream().toList();

        assertEquals("inputTopic", topicPartitions.get(0).topic());
        assertEquals(0, topicPartitions.get(0).partition());

        assertEquals("dlqTopic", KafkaStreamsExecutionContext.getDlqTopicName());
        assertEquals("org.apache.kafka.common.serialization.Serdes$StringSerde",
            KafkaStreamsExecutionContext.getSerdeConfig().get("default.key.serde"));
        assertEquals("org.apache.kafka.common.serialization.Serdes$StringSerde",
            KafkaStreamsExecutionContext.getSerdeConfig().get("default.value.serde"));

        assertEquals("localhost:8080",
            KafkaStreamsExecutionContext.getProperties().get("application.server"));

        // Assert HTTP probes
        HttpRequest requestReady = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8080/ready"))
            .GET()
            .build();

        HttpResponse<Void> responseReady = httpClient.send(requestReady, HttpResponse.BodyHandlers.discarding());

        assertEquals(200, responseReady.statusCode());

        HttpRequest requestLiveness = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8080/liveness"))
            .GET()
            .build();

        HttpResponse<Void> responseLiveness = httpClient.send(requestLiveness, HttpResponse.BodyHandlers.discarding());

        assertEquals(200, responseLiveness.statusCode());

        HttpRequest requestTopology = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8080/topology"))
            .GET()
            .build();

        HttpResponse<String> responseTopology = httpClient.send(requestTopology, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, responseTopology.statusCode());
        assertEquals("""
            Topologies:
               Sub-topology: 0
                Source: KSTREAM-SOURCE-0000000000 (topics: [inputTopic])
                  --> KSTREAM-SINK-0000000001
                Sink: KSTREAM-SINK-0000000001 (topic: outputTopic)
                  <-- KSTREAM-SOURCE-0000000000

            """, responseTopology.body());
    }

    private void waitingForKafkaStreamsToRun() throws InterruptedException {
        while (!initializer.getKafkaStreams().state().equals(KafkaStreams.State.RUNNING)) {
            log.info("Waiting for Kafka Streams to start...");
            Thread.sleep(2000);
        }
    }

    private static void createTopics(String... topics) {
        var newTopics = Arrays.stream(topics)
            .map(topic -> new NewTopic(topic, 1, (short) 1))
            .toList();
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(newTopics);
        }
    }

    static class KafkaStreamInitializerImpl extends KafkaStreamsInitializer {
        @Override
        protected void initProperties() {
            super.initProperties();
            KafkaStreamsExecutionContext.getProperties()
                .setProperty(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        }
    }

    @Slf4j
    static class KafkaStreamsStarterImpl extends KafkaStreamsStarter {
        @Override
        public void topology(StreamsBuilder streamsBuilder) {
            streamsBuilder
                .stream("inputTopic")
                .to("outputTopic");
        }

        @Override
        public String dlqTopic() {
            return "dlqTopic";
        }

        @Override
        public void onStart(KafkaStreams kafkaStreams) {
            log.info("Starting Kafka Streams from integration tests!");
        }
    }
}
