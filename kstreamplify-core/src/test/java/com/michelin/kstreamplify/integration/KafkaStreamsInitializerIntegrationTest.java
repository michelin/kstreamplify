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

import static com.michelin.kstreamplify.property.PropertiesUtils.KAFKA_PROPERTIES_PREFIX;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.integration.container.KafkaIntegrationTest;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class KafkaStreamsInitializerIntegrationTest extends KafkaIntegrationTest {

    @BeforeAll
    static void globalSetUp() {
        createTopics(
                broker.getBootstrapServers(),
                new TopicPartition("INPUT_TOPIC", 2),
                new TopicPartition("OUTPUT_TOPIC", 2));

        Properties properties = new Properties();
        properties.put(KAFKA_PROPERTIES_PREFIX + BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers());

        initializer = new KafkaStreamInitializerStub(new KafkaStreamsStarterStub(), 8080, properties);

        initializer.start();
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        waitingForKafkaStreamsToStart();
    }

    @Test
    void shouldStartAndRun() throws InterruptedException, IOException {
        assertEquals(KafkaStreams.State.RUNNING, initializer.getKafkaStreams().state());

        List<StreamsMetadata> streamsMetadata =
                new ArrayList<>(initializer.getKafkaStreams().metadataForAllStreamsClients());

        // Assert Kafka Streams initialization
        assertEquals("localhost", streamsMetadata.get(0).hostInfo().host());
        assertEquals(8080, streamsMetadata.get(0).hostInfo().port());
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
                "localhost:8080", KafkaStreamsExecutionContext.getProperties().get("application.server"));

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
