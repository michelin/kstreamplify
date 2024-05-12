package com.michelin.kstreamplify.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.DEFINED_PORT;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
@SpringBootTest(webEnvironment = DEFINED_PORT)
class SpringBootKafkaStreamsInitializerIntegrationTest extends KafkaIntegrationTest {
    @Autowired
    private MeterRegistry registry;

    @BeforeAll
    static void setUp() {
        createTopics("inputTopic", "outputTopic");
    }

    @Test
    void shouldInitAndRun() throws InterruptedException {
        waitingForKafkaStreamsToStart();

        assertEquals(KafkaStreams.State.RUNNING, initializer.getKafkaStreams().state());

        List<StreamsMetadata> streamsMetadata =
            new ArrayList<>(initializer.getKafkaStreams().metadataForAllStreamsClients());

        // Assert Kafka Streams initialization
        assertEquals("localhost", streamsMetadata.get(0).hostInfo().host());
        assertEquals(8081, streamsMetadata.get(0).hostInfo().port());
        assertTrue(streamsMetadata.get(0).stateStoreNames().isEmpty());

        List<TopicPartition> topicPartitions = streamsMetadata.get(0).topicPartitions().stream().toList();

        assertEquals("inputTopic", topicPartitions.get(0).topic());
        assertEquals(0, topicPartitions.get(0).partition());

        assertEquals("dlqTopic", KafkaStreamsExecutionContext.getDlqTopicName());
        assertEquals("org.apache.kafka.common.serialization.Serdes$StringSerde",
            KafkaStreamsExecutionContext.getSerdeConfig().get("default.key.serde"));
        assertEquals("org.apache.kafka.common.serialization.Serdes$StringSerde",
            KafkaStreamsExecutionContext.getSerdeConfig().get("default.value.serde"));

        assertEquals("localhost:8081",
            KafkaStreamsExecutionContext.getProperties().get("application.server"));

        // Assert HTTP probes
        ResponseEntity<Void> responseReady = restTemplate
            .getForEntity("http://localhost:8081/ready", Void.class);

        assertEquals(200, responseReady.getStatusCode().value());

        ResponseEntity<Void> responseLiveness = restTemplate
            .getForEntity("http://localhost:8081/liveness", Void.class);

        assertEquals(200, responseLiveness.getStatusCode().value());

        ResponseEntity<String> responseTopology = restTemplate
            .getForEntity("http://localhost:8081/topology", String.class);

        assertEquals(200, responseTopology.getStatusCode().value());
        assertEquals("""
            Topologies:
               Sub-topology: 0
                Source: KSTREAM-SOURCE-0000000000 (topics: [inputTopic])
                  --> KSTREAM-SINK-0000000001
                Sink: KSTREAM-SINK-0000000001 (topic: outputTopic)
                  <-- KSTREAM-SOURCE-0000000000

            """, responseTopology.getBody());
    }

    @Test
    void shouldRegisterKafkaMetrics() throws InterruptedException {
        waitingForKafkaStreamsToStart();

        // Kafka Streams metrics are registered
        assertFalse(registry.getMeters()
            .stream()
            .filter(metric -> metric.getId().getName().startsWith("kafka.stream"))
            .toList()
            .isEmpty());

        // Kafka producer metrics are registered
        assertFalse(registry.getMeters()
            .stream()
            .filter(metric -> metric.getId().getName().startsWith("kafka.producer"))
            .toList()
            .isEmpty());

        // Kafka consumer metrics are registered
        assertFalse(registry.getMeters()
            .stream()
            .filter(metric -> metric.getId().getName().startsWith("kafka.consumer"))
            .toList()
            .isEmpty());
    }

    /**
     * Kafka Streams starter implementation for integration tests.
     * The topology simply forwards messages from inputTopic to outputTopic.
     */
    @Slf4j
    @SpringBootApplication
    static class KafkaStreamsStarterImpl extends KafkaStreamsStarter {
        public static void main(String[] args) {
            SpringApplication.run(KafkaStreamsStarterImpl.class, args);
        }

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
