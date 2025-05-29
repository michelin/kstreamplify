/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.kstreamplify.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.DEFINED_PORT;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.integration.container.KafkaIntegrationTest;
import io.micrometer.core.instrument.MeterRegistry;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
@SpringBootTest(webEnvironment = DEFINED_PORT)
class SpringBootKafkaStreamsInitializerIntegrationTest extends KafkaIntegrationTest {
    @Autowired
    private MeterRegistry registry;

    @BeforeAll
    static void globalSetUp() {
        createTopics(
                broker.getBootstrapServers(),
                new TopicPartition("INPUT_TOPIC", 2),
                new TopicPartition("OUTPUT_TOPIC", 2));
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        waitingForKafkaStreamsToStart();
    }

    @Test
    void shouldStartAndRun() {
        assertEquals(KafkaStreams.State.RUNNING, initializer.getKafkaStreams().state());

        List<StreamsMetadata> streamsMetadata =
                new ArrayList<>(initializer.getKafkaStreams().metadataForAllStreamsClients());

        // Assert Kafka Streams initialization
        assertEquals("localhost", streamsMetadata.get(0).hostInfo().host());
        assertEquals(8000, streamsMetadata.get(0).hostInfo().port());
        assertTrue(streamsMetadata.get(0).stateStoreNames().isEmpty());

        Set<TopicPartition> topicPartitions = streamsMetadata.get(0).topicPartitions();

        assertTrue(Set.of(new TopicPartition("INPUT_TOPIC", 0), new TopicPartition("INPUT_TOPIC", 1))
                .containsAll(topicPartitions));

        assertEquals("DLQ_TOPIC", KafkaStreamsExecutionContext.getDlqTopicName());
        assertEquals(
                "org.apache.kafka.common.serialization.Serdes$StringSerde",
                KafkaStreamsExecutionContext.getSerdesConfig().get("default.key.serde"));
        assertEquals(
                "org.apache.kafka.common.serialization.Serdes$StringSerde",
                KafkaStreamsExecutionContext.getSerdesConfig().get("default.value.serde"));

        assertEquals(
                "localhost:8000", KafkaStreamsExecutionContext.getProperties().get("application.server"));

        // Assert HTTP probes
        ResponseEntity<Void> responseReady = restTemplate.getForEntity("http://localhost:8000/ready", Void.class);

        assertEquals(200, responseReady.getStatusCode().value());

        ResponseEntity<Void> responseLiveness = restTemplate.getForEntity("http://localhost:8000/liveness", Void.class);

        assertEquals(200, responseLiveness.getStatusCode().value());

        ResponseEntity<String> responseTopology =
                restTemplate.getForEntity("http://localhost:8000/topology", String.class);

        assertEquals(200, responseTopology.getStatusCode().value());
        assertEquals(
                """
            Topologies:
               Sub-topology: 0
                Source: KSTREAM-SOURCE-0000000000 (topics: [INPUT_TOPIC])
                  --> KSTREAM-SINK-0000000001
                Sink: KSTREAM-SINK-0000000001 (topic: OUTPUT_TOPIC)
                  <-- KSTREAM-SOURCE-0000000000

            """,
                responseTopology.getBody());
    }

    @Test
    void shouldRegisterKafkaMetrics() {
        // Kafka Streams metrics are registered
        assertFalse(registry.getMeters().stream()
                .filter(metric -> metric.getId().getName().startsWith("kafka.stream"))
                .toList()
                .isEmpty());

        // Kafka producer metrics are registered
        assertFalse(registry.getMeters().stream()
                .filter(metric -> metric.getId().getName().startsWith("kafka.producer"))
                .toList()
                .isEmpty());

        // Kafka consumer metrics are registered
        assertFalse(registry.getMeters().stream()
                .filter(metric -> metric.getId().getName().startsWith("kafka.consumer"))
                .toList()
                .isEmpty());
    }

    /**
     * Kafka Streams starter implementation for integration tests. The topology simply forwards messages from inputTopic
     * to outputTopic.
     */
    @Slf4j
    @SpringBootApplication
    static class KafkaStreamsStarterStub extends KafkaStreamsStarter {
        public static void main(String[] args) {
            SpringApplication.run(KafkaStreamsStarterStub.class, args);
        }

        @Override
        public void topology(StreamsBuilder streamsBuilder) {
            streamsBuilder.stream("INPUT_TOPIC").to("OUTPUT_TOPIC");
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
