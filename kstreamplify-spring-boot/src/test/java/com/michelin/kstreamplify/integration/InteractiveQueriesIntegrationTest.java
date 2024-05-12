package com.michelin.kstreamplify.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.DEFINED_PORT;
import static org.springframework.http.HttpMethod.GET;

import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.ResponseEntity;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
@SpringBootTest(webEnvironment = DEFINED_PORT)
class InteractiveQueriesIntegrationTest extends KafkaIntegrationTest {
    @BeforeAll
    static void setUp() {
        createTopics("stringTopic");
    }

    @Test
    void shouldGetStores() throws InterruptedException {
        waitingForKafkaStreamsToStart();

        assertEquals(KafkaStreams.State.RUNNING, initializer.getKafkaStreams().state());

        ResponseEntity<List<String>> stores = restTemplate
            .exchange("http://localhost:8081/store", GET, null, new ParameterizedTypeReference<>() {
            });

        assertEquals(200, stores.getStatusCode().value());
        assertEquals(List.of("STRING_STORE", "JAVA_STORE", "AVRO_STORE"), stores.getBody());
    }

    /**
     * Kafka Streams starter implementation for integration tests.
     * The topology consumes events from multiple topics (string, Java, Avro) and stores them in dedicated stores
     * so that they can be queried.
     */
    @Slf4j
    @SpringBootApplication
    static class KafkaStreamsStarterImpl extends KafkaStreamsStarter {
        public static void main(String[] args) {
            SpringApplication.run(SpringBootKafkaStreamsInitializerIntegrationTest.KafkaStreamsStarterImpl.class, args);
        }

        @Override
        public void topology(StreamsBuilder streamsBuilder) {
            streamsBuilder
                .table("stringTopic",
                    Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("STRING_STORE"));

            streamsBuilder
                .table("javaTopic",
                    Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("JAVA_STORE"));

            streamsBuilder
                .table("avroTopic",
                    Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("AVRO_STORE"));
        }

        @Override
        public String dlqTopic() {
            return "dlqTopic";
        }

        @Override
        public void onStart(KafkaStreams kafkaStreams) {
            kafkaStreams.cleanUp();
        }
    }
}
