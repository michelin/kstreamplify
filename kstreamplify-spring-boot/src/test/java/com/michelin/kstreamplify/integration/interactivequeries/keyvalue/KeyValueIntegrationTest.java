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
package com.michelin.kstreamplify.integration.interactivequeries.keyvalue;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.DEFINED_PORT;

import com.michelin.kstreamplify.avro.KafkaUserStub;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.integration.container.KafkaIntegrationTest;
import com.michelin.kstreamplify.serde.SerdesUtils;
import com.michelin.kstreamplify.service.interactivequeries.keyvalue.KeyValueStoreService;
import com.michelin.kstreamplify.store.StateStoreRecord;
import com.michelin.kstreamplify.store.StreamsMetadata;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
@ActiveProfiles("interactive-queries-key-value")
@SpringBootTest(webEnvironment = DEFINED_PORT)
class KeyValueIntegrationTest extends KafkaIntegrationTest {
    @Autowired
    private KeyValueStoreService keyValueService;

    @BeforeAll
    static void globalSetUp() throws ExecutionException, InterruptedException {
        createTopics(
                broker.getBootstrapServers(),
                new TopicPartition("STRING_TOPIC", 3),
                new TopicPartition("AVRO_TOPIC", 2));

        try (KafkaProducer<String, String> stringKafkaProducer = new KafkaProducer<>(Map.of(
                BOOTSTRAP_SERVERS_CONFIG,
                broker.getBootstrapServers(),
                KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()))) {

            ProducerRecord<String, String> message = new ProducerRecord<>("STRING_TOPIC", "user", "Doe");

            stringKafkaProducer.send(message).get();
        }

        try (KafkaProducer<String, KafkaUserStub> avroKafkaProducer = new KafkaProducer<>(Map.of(
                BOOTSTRAP_SERVERS_CONFIG,
                broker.getBootstrapServers(),
                KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName(),
                SCHEMA_REGISTRY_URL_CONFIG,
                "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getFirstMappedPort()))) {

            KafkaUserStub kafkaUserStub = KafkaUserStub.newBuilder()
                    .setId(1L)
                    .setFirstName("John")
                    .setLastName("Doe")
                    .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
                    .build();

            ProducerRecord<String, KafkaUserStub> message = new ProducerRecord<>("AVRO_TOPIC", "user", kafkaUserStub);

            avroKafkaProducer.send(message).get();
        }
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        waitingForKafkaStreamsToStart();
        waitingForLocalStoreToReachOffset(Map.of(
                "STRING_STRING_KV_STORE", Map.of(2, 1L),
                "STRING_AVRO_KV_STORE", Map.of(0, 1L),
                "STRING_AVRO_WINDOW_STORE", Map.of(0, 1L)));
    }

    @Test
    void shouldGetStoresAndStoreMetadata() {
        // Get stores
        List<String> stores = restTemplate
                .get()
                .uri("http://localhost:8002/store")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(new ParameterizedTypeReference<List<String>>() {})
                .returnResult()
                .getResponseBody();

        assertNotNull(stores);
        assertTrue(stores.containsAll(
                List.of("STRING_STRING_KV_STORE", "STRING_AVRO_KV_STORE", "STRING_AVRO_WINDOW_STORE")));

        // Get hosts
        List<StreamsMetadata> streamsMetadata = restTemplate
                .get()
                .uri("http://localhost:8002/store/metadata/STRING_STRING_KV_STORE")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(new ParameterizedTypeReference<List<StreamsMetadata>>() {})
                .returnResult()
                .getResponseBody();

        assertNotNull(streamsMetadata);
        assertEquals(
                Set.of("STRING_STRING_KV_STORE", "STRING_AVRO_KV_STORE", "STRING_AVRO_WINDOW_STORE"),
                streamsMetadata.get(0).getStateStoreNames());
        assertEquals("localhost", streamsMetadata.get(0).getHostInfo().host());
        assertEquals(8002, streamsMetadata.get(0).getHostInfo().port());
        assertEquals(
                Set.of("AVRO_TOPIC-0", "AVRO_TOPIC-1", "STRING_TOPIC-0", "STRING_TOPIC-1", "STRING_TOPIC-2"),
                streamsMetadata.get(0).getTopicPartitions());
    }

    @ParameterizedTest
    @CsvSource({
        "http://localhost:8002/store/key-value/WRONG_STORE/user,State store WRONG_STORE not found",
        "http://localhost:8002/store/key-value/STRING_STRING_KV_STORE/wrongKey,Key wrongKey not found",
        "http://localhost:8002/store/key-value/WRONG_STORE,State store WRONG_STORE not found"
    })
    void shouldNotFoundWhenKeyOrStoreNotFound(String url, String message) {
        String response = restTemplate
                .get()
                .uri(url)
                .exchange()
                .expectStatus()
                .isNotFound()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals(message, response);
    }

    @Test
    void shouldGetErrorWhenQueryingWrongStoreType() {
        String response = restTemplate
                .get()
                .uri("http://localhost:8002/store/key-value/STRING_AVRO_WINDOW_STORE/user")
                .exchange()
                .expectStatus()
                .isBadRequest()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertNotNull(response);
    }

    @Test
    void shouldGetByKeyInStringStringStore() {
        StateStoreRecord response = restTemplate
                .get()
                .uri("http://localhost:8002/store/key-value/STRING_STRING_KV_STORE/user")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(StateStoreRecord.class)
                .returnResult()
                .getResponseBody();

        assertNotNull(response);
        assertEquals("user", response.getKey());
        assertEquals("Doe", response.getValue());
        assertNull(response.getTimestamp());
    }

    @Test
    void shouldGetByKeyInStringAvroStore() {
        StateStoreRecord response = restTemplate
                .get()
                .uri("http://localhost:8002/store/key-value/STRING_AVRO_KV_STORE/user")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(StateStoreRecord.class)
                .returnResult()
                .getResponseBody();

        assertNotNull(response);
        assertEquals("user", response.getKey());
        assertEquals(1, ((HashMap<?, ?>) response.getValue()).get("id"));
        assertEquals("John", ((HashMap<?, ?>) response.getValue()).get("firstName"));
        assertEquals("Doe", ((HashMap<?, ?>) response.getValue()).get("lastName"));
        assertEquals("2000-01-01T01:00:00Z", ((HashMap<?, ?>) response.getValue()).get("birthDate"));
        assertNull(response.getTimestamp());
    }

    @ParameterizedTest
    @CsvSource({
        "http://localhost:8002/store/key-value/STRING_STRING_KV_STORE",
        "http://localhost:8002/store/key-value/local/STRING_STRING_KV_STORE"
    })
    void shouldGetAllInStringStringStore(String url) {
        List<StateStoreRecord> response = restTemplate
                .get()
                .uri(url)
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(new ParameterizedTypeReference<List<StateStoreRecord>>() {})
                .returnResult()
                .getResponseBody();

        assertNotNull(response);
        assertEquals("user", response.get(0).getKey());
        assertEquals("Doe", response.get(0).getValue());
        assertNull(response.get(0).getTimestamp());
    }

    @ParameterizedTest
    @CsvSource({
        "http://localhost:8002/store/key-value/STRING_AVRO_KV_STORE",
        "http://localhost:8002/store/key-value/local/STRING_AVRO_KV_STORE"
    })
    void shouldGetAllFromStringAvroStores(String url) {
        List<StateStoreRecord> response = restTemplate
                .get()
                .uri(url)
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(new ParameterizedTypeReference<List<StateStoreRecord>>() {})
                .returnResult()
                .getResponseBody();

        assertNotNull(response);
        assertEquals("user", response.get(0).getKey());
        assertEquals(1, ((Map<?, ?>) response.get(0).getValue()).get("id"));
        assertEquals("John", ((Map<?, ?>) response.get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) response.get(0).getValue()).get("lastName"));
        assertEquals("2000-01-01T01:00:00Z", ((Map<?, ?>) response.get(0).getValue()).get("birthDate"));
        assertNull(response.get(0).getTimestamp());
    }

    @Test
    void shouldGetByKeyInStringAvroStoreFromService() {
        StateStoreRecord stateStoreRecord = keyValueService.getByKey("STRING_AVRO_KV_STORE", "user");

        assertEquals("user", stateStoreRecord.getKey());
        assertEquals(1L, ((Map<?, ?>) stateStoreRecord.getValue()).get("id"));
        assertEquals("John", ((Map<?, ?>) stateStoreRecord.getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) stateStoreRecord.getValue()).get("lastName"));
        assertEquals("2000-01-01T01:00:00Z", ((Map<?, ?>) stateStoreRecord.getValue()).get("birthDate"));
        assertNull(stateStoreRecord.getTimestamp());
    }

    @Test
    void shouldGetAllInStringAvroStoreFromService() {
        List<StateStoreRecord> stateQueryData = keyValueService.getAll("STRING_AVRO_KV_STORE");

        assertEquals("user", stateQueryData.get(0).getKey());
        assertEquals(1L, ((Map<?, ?>) stateQueryData.get(0).getValue()).get("id"));
        assertEquals("John", ((Map<?, ?>) stateQueryData.get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) stateQueryData.get(0).getValue()).get("lastName"));
        assertEquals("2000-01-01T01:00:00Z", ((Map<?, ?>) stateQueryData.get(0).getValue()).get("birthDate"));
        assertNull(stateQueryData.get(0).getTimestamp());
    }

    /**
     * Kafka Streams starter implementation for integration tests. The topology consumes events from multiple topics and
     * stores them in dedicated stores so that they can be queried.
     */
    @Slf4j
    @SpringBootApplication
    static class KafkaStreamsStarterStub extends KafkaStreamsStarter {
        public static void main(String[] args) {
            SpringApplication.run(KafkaStreamsStarterStub.class, args);
        }

        @Override
        public void topology(StreamsBuilder streamsBuilder) {
            streamsBuilder.stream("STRING_TOPIC", Consumed.with(Serdes.String(), Serdes.String()))
                    .process(new ProcessorSupplier<String, String, String, String>() {
                        @Override
                        public Set<StoreBuilder<?>> stores() {
                            StoreBuilder<KeyValueStore<String, String>> stringStringKeyValueStoreBuilder =
                                    Stores.keyValueStoreBuilder(
                                            Stores.persistentKeyValueStore("STRING_STRING_KV_STORE"),
                                            Serdes.String(),
                                            Serdes.String());

                            return Set.of(stringStringKeyValueStoreBuilder);
                        }

                        @Override
                        public Processor<String, String, String, String> get() {
                            return new Processor<>() {
                                private KeyValueStore<String, String> stringStringKeyValueStore;

                                @Override
                                public void init(ProcessorContext<String, String> context) {
                                    this.stringStringKeyValueStore = context.getStateStore("STRING_STRING_KV_STORE");
                                }

                                @Override
                                public void process(Record<String, String> message) {
                                    stringStringKeyValueStore.put(message.key(), message.value());
                                }
                            };
                        }
                    });

            streamsBuilder.stream(
                            "AVRO_TOPIC", Consumed.with(Serdes.String(), SerdesUtils.<KafkaUserStub>getValueSerdes()))
                    .process(new ProcessorSupplier<String, KafkaUserStub, String, KafkaUserStub>() {
                        @Override
                        public Set<StoreBuilder<?>> stores() {
                            StoreBuilder<KeyValueStore<String, KafkaUserStub>> stringAvroKeyValueStoreBuilder =
                                    Stores.keyValueStoreBuilder(
                                            Stores.persistentKeyValueStore("STRING_AVRO_KV_STORE"),
                                            Serdes.String(),
                                            SerdesUtils.getValueSerdes());

                            StoreBuilder<WindowStore<String, KafkaUserStub>> stringAvroWindowStoreBuilder =
                                    Stores.windowStoreBuilder(
                                            Stores.persistentWindowStore(
                                                    "STRING_AVRO_WINDOW_STORE",
                                                    Duration.ofMinutes(5),
                                                    Duration.ofMinutes(1),
                                                    false),
                                            Serdes.String(),
                                            SerdesUtils.getValueSerdes());

                            return Set.of(stringAvroKeyValueStoreBuilder, stringAvroWindowStoreBuilder);
                        }

                        @Override
                        public Processor<String, KafkaUserStub, String, KafkaUserStub> get() {
                            return new Processor<>() {
                                private KeyValueStore<String, KafkaUserStub> stringAvroKeyValueStore;
                                private WindowStore<String, KafkaUserStub> stringAvroWindowStore;

                                @Override
                                public void init(ProcessorContext<String, KafkaUserStub> context) {
                                    this.stringAvroKeyValueStore = context.getStateStore("STRING_AVRO_KV_STORE");
                                    this.stringAvroWindowStore = context.getStateStore("STRING_AVRO_WINDOW_STORE");
                                }

                                @Override
                                public void process(Record<String, KafkaUserStub> message) {
                                    stringAvroKeyValueStore.put(message.key(), message.value());
                                    stringAvroWindowStore.put(message.key(), message.value(), message.timestamp());
                                }
                            };
                        }
                    });
        }

        @Override
        public String dlqTopic() {
            return "DLQ_TOPIC";
        }

        @Override
        public void onStart(KafkaStreams kafkaStreams) {
            kafkaStreams.cleanUp();
        }
    }
}
