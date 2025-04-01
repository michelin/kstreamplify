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
package com.michelin.kstreamplify.integration.interactivequeries.window;

import static com.michelin.kstreamplify.property.PropertiesUtils.KAFKA_PROPERTIES_PREFIX;
import static com.michelin.kstreamplify.property.PropertiesUtils.PROPERTY_SEPARATOR;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.michelin.kstreamplify.avro.KafkaUserStub;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.integration.container.KafkaIntegrationTest;
import com.michelin.kstreamplify.property.PropertiesUtils;
import com.michelin.kstreamplify.serde.SerdesUtils;
import com.michelin.kstreamplify.service.interactivequeries.window.WindowStoreService;
import com.michelin.kstreamplify.store.StateStoreRecord;
import com.michelin.kstreamplify.store.StreamsMetadata;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class WindowIntegrationTest extends KafkaIntegrationTest {
    private final WindowStoreService windowService = new WindowStoreService(initializer);

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

        Properties properties = PropertiesUtils.loadProperties();
        properties.putAll(
            Map.of(
                KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + BOOTSTRAP_SERVERS_CONFIG,
                broker.getBootstrapServers(),
                KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + APPLICATION_ID_CONFIG,
                "appWindowInteractiveQueriesId",
                KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + SCHEMA_REGISTRY_URL_CONFIG,
                "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getFirstMappedPort(),
                KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + STATE_DIR_CONFIG,
                "/tmp/kstreamplify/kstreamplify-core-test/interactive-queries/window")
        );

        initializer = new KafkaStreamInitializerStub(new KafkaStreamsStarterStub(), 8085, properties);
        initializer.startKafkaStreams();
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        waitingForKafkaStreamsToStart();
        waitingForLocalStoreToReachOffset(Map.of(
                "STRING_STRING_WINDOW_STORE", Map.of(1, 1L),
                "STRING_AVRO_WINDOW_STORE", Map.of(0, 1L),
                "STRING_AVRO_KV_STORE", Map.of(0, 1L)));
    }

    @Test
    void shouldGetStoresAndStoreMetadata() throws IOException, InterruptedException {
        // Get stores
        HttpRequest storesRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8085/store"))
                .GET()
                .build();

        HttpResponse<String> storesResponse = httpClient.send(storesRequest, HttpResponse.BodyHandlers.ofString());
        List<String> stores = objectMapper.readValue(storesResponse.body(), new TypeReference<>() {});

        assertEquals(200, storesResponse.statusCode());
        assertTrue(stores.containsAll(
                List.of("STRING_STRING_WINDOW_STORE", "STRING_AVRO_WINDOW_STORE", "STRING_AVRO_KV_STORE")));

        // Get store metadata
        HttpRequest streamsMetadataRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8085/store/metadata/STRING_STRING_WINDOW_STORE"))
                .GET()
                .build();

        HttpResponse<String> streamsMetadataResponse =
                httpClient.send(streamsMetadataRequest, HttpResponse.BodyHandlers.ofString());

        List<StreamsMetadata> streamsMetadata =
                objectMapper.readValue(streamsMetadataResponse.body(), new TypeReference<>() {});

        assertEquals(200, streamsMetadataResponse.statusCode());
        assertEquals(
                Set.of("STRING_STRING_WINDOW_STORE", "STRING_AVRO_WINDOW_STORE", "STRING_AVRO_KV_STORE"),
                streamsMetadata.get(0).getStateStoreNames());
        assertEquals("localhost", streamsMetadata.get(0).getHostInfo().host());
        assertEquals(8085, streamsMetadata.get(0).getHostInfo().port());
        assertEquals(
                Set.of("AVRO_TOPIC-0", "AVRO_TOPIC-1", "STRING_TOPIC-0", "STRING_TOPIC-1", "STRING_TOPIC-2"),
                streamsMetadata.get(0).getTopicPartitions());
    }

    @ParameterizedTest
    @CsvSource({
        "http://localhost:8085/store/window/WRONG_STORE/user,State store WRONG_STORE not found",
        "http://localhost:8085/store/window/STRING_STRING_WINDOW_STORE/wrongKey,Key wrongKey not found",
        "http://localhost:8085/store/window/WRONG_STORE,State store WRONG_STORE not found"
    })
    void shouldNotFoundWhenKeyOrStoreNotFound(String url, String message) throws IOException, InterruptedException {
        HttpRequest request =
                HttpRequest.newBuilder().uri(URI.create(url)).GET().build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(404, response.statusCode());
        assertEquals(message, response.body());
    }

    @Test
    void shouldGetErrorWhenQueryingWrongStoreType() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8085/store/window/STRING_AVRO_KV_STORE/user"))
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(400, response.statusCode());
        assertNotNull(response.body());
    }

    @Test
    void shouldGetByKeyInStringStringStore() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8085/store/window/STRING_STRING_WINDOW_STORE/user"))
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        List<StateStoreRecord> body = objectMapper.readValue(response.body(), new TypeReference<>() {});

        assertEquals(200, response.statusCode());
        assertEquals("user", body.get(0).getKey());
        assertEquals("Doe", body.get(0).getValue());
        assertNull(body.get(0).getTimestamp());
    }

    @Test
    void shouldGetByKeyInStringAvroStore() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8085/store/window/STRING_AVRO_WINDOW_STORE/user"))
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        List<StateStoreRecord> body = objectMapper.readValue(response.body(), new TypeReference<>() {});

        assertEquals(200, response.statusCode());
        assertEquals("user", body.get(0).getKey());
        assertEquals(1, ((Map<?, ?>) body.get(0).getValue()).get("id"));
        assertEquals("John", ((Map<?, ?>) body.get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) body.get(0).getValue()).get("lastName"));
        assertEquals("2000-01-01T01:00:00Z", ((Map<?, ?>) body.get(0).getValue()).get("birthDate"));
        assertNull(body.get(0).getTimestamp());
    }

    @ParameterizedTest
    @CsvSource({
        "http://localhost:8085/store/window/STRING_STRING_WINDOW_STORE/user",
        "http://localhost:8085/store/window/STRING_AVRO_WINDOW_STORE/user"
    })
    void shouldNotFoundWhenStartTimeIsTooLate(String url) throws IOException, InterruptedException {
        Instant tooLate = Instant.now().plus(Duration.ofDays(1));
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url + "?startTime=" + tooLate))
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(404, response.statusCode());
    }

    @ParameterizedTest
    @CsvSource({
        "http://localhost:8085/store/window/STRING_STRING_WINDOW_STORE/user",
        "http://localhost:8085/store/window/STRING_AVRO_WINDOW_STORE/user"
    })
    void shouldNotFoundWhenEndTimeIsTooEarly(String url) throws IOException, InterruptedException {
        Instant tooEarly = Instant.now().minus(Duration.ofDays(1));
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url + "?endTime=" + tooEarly))
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(404, response.statusCode());
    }

    @ParameterizedTest
    @CsvSource({
        "http://localhost:8085/store/window/STRING_STRING_WINDOW_STORE",
        "http://localhost:8085/store/window/local/STRING_STRING_WINDOW_STORE"
    })
    void shouldGetAllInStringStringStore(String url) throws IOException, InterruptedException {
        HttpRequest request =
                HttpRequest.newBuilder().uri(URI.create(url)).GET().build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        List<StateStoreRecord> body = objectMapper.readValue(response.body(), new TypeReference<>() {});

        assertEquals(200, response.statusCode());
        assertEquals("user", body.get(0).getKey());
        assertEquals("Doe", body.get(0).getValue());
        assertNull(body.get(0).getTimestamp());
    }

    @ParameterizedTest
    @CsvSource({
        "http://localhost:8085/store/window/STRING_AVRO_WINDOW_STORE",
        "http://localhost:8085/store/window/local/STRING_AVRO_WINDOW_STORE"
    })
    void shouldGetAllFromStringAvroStores(String url) throws IOException, InterruptedException {
        HttpRequest request =
                HttpRequest.newBuilder().uri(URI.create(url)).GET().build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        List<StateStoreRecord> body = objectMapper.readValue(response.body(), new TypeReference<>() {});

        assertEquals(200, response.statusCode());
        assertEquals("user", body.get(0).getKey());
        assertEquals(1, ((Map<?, ?>) body.get(0).getValue()).get("id"));
        assertEquals("John", ((Map<?, ?>) body.get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) body.get(0).getValue()).get("lastName"));
        assertEquals("2000-01-01T01:00:00Z", ((Map<?, ?>) body.get(0).getValue()).get("birthDate"));
        assertNull(body.get(0).getTimestamp());
    }

    @Test
    void shouldGetByKeyInStringAvroStoreFromService() {
        List<StateStoreRecord> stateStoreRecord =
                windowService.getByKey("STRING_AVRO_WINDOW_STORE", "user", Instant.EPOCH, Instant.now());

        assertEquals("user", stateStoreRecord.get(0).getKey());
        assertEquals(1L, ((Map<?, ?>) stateStoreRecord.get(0).getValue()).get("id"));
        assertEquals("John", ((Map<?, ?>) stateStoreRecord.get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) stateStoreRecord.get(0).getValue()).get("lastName"));
        assertEquals(
                "2000-01-01T01:00:00Z", ((Map<?, ?>) stateStoreRecord.get(0).getValue()).get("birthDate"));
        assertNull(stateStoreRecord.get(0).getTimestamp());
    }

    @Test
    void shouldGetAllInStringAvroStoreFromService() {
        List<StateStoreRecord> stateQueryData =
                windowService.getAll("STRING_AVRO_WINDOW_STORE", Instant.EPOCH, Instant.now());

        assertEquals("user", stateQueryData.get(0).getKey());
        assertEquals(1L, ((Map<?, ?>) stateQueryData.get(0).getValue()).get("id"));
        assertEquals("John", ((Map<?, ?>) stateQueryData.get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) stateQueryData.get(0).getValue()).get("lastName"));
        assertEquals("2000-01-01T01:00:00Z", ((Map<?, ?>) stateQueryData.get(0).getValue()).get("birthDate"));
        assertNull(stateQueryData.get(0).getTimestamp());
    }

    /**
     * Kafka Streams starter implementation for integration tests. The topology consumes events from multiple topics
     * (string, Java, Avro) and stores them in dedicated stores so that they can be queried.
     */
    @Slf4j
    static class KafkaStreamsStarterStub extends KafkaStreamsStarter {
        @Override
        public void topology(StreamsBuilder streamsBuilder) {
            streamsBuilder.stream("STRING_TOPIC", Consumed.with(Serdes.String(), Serdes.String()))
                    .process(new ProcessorSupplier<String, String, String, String>() {
                        @Override
                        public Set<StoreBuilder<?>> stores() {
                            StoreBuilder<WindowStore<String, String>> stringStringWindowStoreBuilder =
                                    Stores.windowStoreBuilder(
                                            Stores.persistentWindowStore(
                                                    "STRING_STRING_WINDOW_STORE",
                                                    Duration.ofMinutes(5),
                                                    Duration.ofMinutes(1),
                                                    false),
                                            Serdes.String(),
                                            Serdes.String());

                            return Set.of(stringStringWindowStoreBuilder);
                        }

                        @Override
                        public Processor<String, String, String, String> get() {
                            return new Processor<>() {
                                private WindowStore<String, String> stringStringWindowStore;

                                @Override
                                public void init(ProcessorContext<String, String> context) {
                                    this.stringStringWindowStore = context.getStateStore("STRING_STRING_WINDOW_STORE");
                                }

                                @Override
                                public void process(Record<String, String> message) {
                                    stringStringWindowStore.put(message.key(), message.value(), message.timestamp());
                                }
                            };
                        }
                    });

            streamsBuilder.stream(
                            "AVRO_TOPIC", Consumed.with(Serdes.String(), SerdesUtils.<KafkaUserStub>getValueSerdes()))
                    .process(new ProcessorSupplier<String, KafkaUserStub, String, KafkaUserStub>() {
                        @Override
                        public Set<StoreBuilder<?>> stores() {
                            StoreBuilder<WindowStore<String, KafkaUserStub>> stringAvroWindowStoreBuilder =
                                    Stores.windowStoreBuilder(
                                            Stores.persistentWindowStore(
                                                    "STRING_AVRO_WINDOW_STORE",
                                                    Duration.ofMinutes(5),
                                                    Duration.ofMinutes(1),
                                                    false),
                                            Serdes.String(),
                                            SerdesUtils.getValueSerdes());

                            StoreBuilder<KeyValueStore<String, KafkaUserStub>> stringAvroKeyValueStoreBuilder =
                                    Stores.keyValueStoreBuilder(
                                            Stores.persistentKeyValueStore("STRING_AVRO_KV_STORE"),
                                            Serdes.String(),
                                            SerdesUtils.getValueSerdes());

                            return Set.of(stringAvroWindowStoreBuilder, stringAvroKeyValueStoreBuilder);
                        }

                        @Override
                        public Processor<String, KafkaUserStub, String, KafkaUserStub> get() {
                            return new Processor<>() {
                                private WindowStore<String, KafkaUserStub> stringAvroWindowStore;
                                private KeyValueStore<String, KafkaUserStub> stringAvroKeyValueStore;

                                @Override
                                public void init(ProcessorContext<String, KafkaUserStub> context) {
                                    this.stringAvroWindowStore = context.getStateStore("STRING_AVRO_WINDOW_STORE");
                                    this.stringAvroKeyValueStore = context.getStateStore("STRING_AVRO_KV_STORE");
                                }

                                @Override
                                public void process(Record<String, KafkaUserStub> message) {
                                    stringAvroWindowStore.put(message.key(), message.value(), message.timestamp());
                                    stringAvroKeyValueStore.put(message.key(), message.value());
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
