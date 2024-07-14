package com.michelin.kstreamplify.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
class KafkaStreamsInitializerIntegrationTest extends KafkaIntegrationTest {
    @Container
    static KafkaContainer broker = new KafkaContainer(DockerImageName
        .parse("confluentinc/cp-kafka:" + CONFLUENT_PLATFORM_VERSION))
        .withNetwork(NETWORK)
        .withNetworkAliases("broker")
        .withKraft();

    @BeforeAll
    static void globalSetUp() {
        createTopics(broker.getBootstrapServers(),
            "INPUT_TOPIC", "OUTPUT_TOPIC");
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        initializer = new KafkaStreamInitializerStub(broker.getBootstrapServers());
        initializer.init(new KafkaStreamsStarterStub());

        waitingForKafkaStreamsToStart();
    }

    @Test
    void shouldInitAndRun() throws InterruptedException, IOException {
        assertEquals(KafkaStreams.State.RUNNING, initializer.getKafkaStreams().state());

        List<StreamsMetadata> streamsMetadata =
            new ArrayList<>(initializer.getKafkaStreams().metadataForAllStreamsClients());

        // Assert Kafka Streams initialization
        assertEquals("localhost", streamsMetadata.get(0).hostInfo().host());
        assertEquals(8080, streamsMetadata.get(0).hostInfo().port());
        assertTrue(streamsMetadata.get(0).stateStoreNames().isEmpty());

        Set<TopicPartition> topicPartitions = streamsMetadata.get(0).topicPartitions();

        assertTrue(Set.of(
            new TopicPartition("INPUT_TOPIC", 0),
            new TopicPartition("INPUT_TOPIC", 1)
        ).containsAll(topicPartitions));

        assertEquals("DLQ_TOPIC", KafkaStreamsExecutionContext.getDlqTopicName());
        assertEquals("org.apache.kafka.common.serialization.Serdes$StringSerde",
            KafkaStreamsExecutionContext.getSerdesConfig().get("default.key.serde"));
        assertEquals("org.apache.kafka.common.serialization.Serdes$StringSerde",
            KafkaStreamsExecutionContext.getSerdesConfig().get("default.value.serde"));

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
                Source: KSTREAM-SOURCE-0000000000 (topics: [INPUT_TOPIC])
                  --> KSTREAM-SINK-0000000001
                Sink: KSTREAM-SINK-0000000001 (topic: OUTPUT_TOPIC)
                  <-- KSTREAM-SOURCE-0000000000

            """, responseTopology.body());
    }

    @Slf4j
    static class KafkaStreamsStarterStub extends KafkaStreamsStarter {
        @Override
        public void topology(StreamsBuilder streamsBuilder) {
            streamsBuilder
                .stream("INPUT_TOPIC")
                .to("OUTPUT_TOPIC");
        }

        @Override
        public String dlqTopic() {
            return "DLQ_TOPIC";
        }

        @Override
        public void onStart(KafkaStreams kafkaStreams) {
            log.info("Starting Kafka Streams from integration tests!");
        }
    }
}
