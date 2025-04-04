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

import com.fasterxml.jackson.core.type.TypeReference;
import com.michelin.kstreamplify.avro.KafkaUserStub;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.integration.container.KafkaIntegrationTest;
import com.michelin.kstreamplify.serde.SerdesUtils;
import com.michelin.kstreamplify.service.interactivequeries.keyvalue.TimestampedKeyValueStoreService;
import com.michelin.kstreamplify.store.StateStoreRecord;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
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
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class TimestampedKeyValueIntegrationTest extends KafkaIntegrationTest {
    private final TimestampedKeyValueStoreService timestampedKeyValueService =
            new TimestampedKeyValueStoreService(initializer);

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

            KafkaUserStub KafkaUserStub = KafkaUserStub.newBuilder()
                    .setId(1L)
                    .setFirstName("John")
                    .setLastName("Doe")
                    .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
                    .build();

            ProducerRecord<String, KafkaUserStub> message = new ProducerRecord<>("AVRO_TOPIC", "user", KafkaUserStub);

            avroKafkaProducer.send(message).get();
        }

        initializer = new KafkaStreamInitializerStub(
                8083,
                Map.of(
                        KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + BOOTSTRAP_SERVERS_CONFIG,
                        broker.getBootstrapServers(),
                        KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + APPLICATION_ID_CONFIG,
                        "appTimestampedKeyValueInteractiveQueriesId",
                        KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + SCHEMA_REGISTRY_URL_CONFIG,
                        "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getFirstMappedPort(),
                        KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + STATE_DIR_CONFIG,
                        "/tmp/kstreamplify/kstreamplify-core-test/interactive-queries/timestamped-key-value"));
        initializer.init(new KafkaStreamsStarterStub());
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        waitingForKafkaStreamsToStart();
        waitingForLocalStoreToReachOffset(Map.of(
                "STRING_STRING_TIMESTAMPED_STORE", Map.of(1, 1L),
                "STRING_AVRO_TIMESTAMPED_STORE", Map.of(0, 1L),
                "STRING_AVRO_WINDOW_STORE", Map.of(0, 1L)));
    }

    @ParameterizedTest
    @CsvSource({
        "http://localhost:8083/store/key-value/timestamped/WRONG_STORE/user,State store WRONG_STORE not found",
        "http://localhost:8083/store/key-value/timestamped/STRING_STRING_TIMESTAMPED_STORE/wrongKey,Key wrongKey not found",
        "http://localhost:8083/store/key-value/timestamped/WRONG_STORE,State store WRONG_STORE not found"
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
                .uri(URI.create("http://localhost:8083/store/key-value/timestamped/STRING_AVRO_WINDOW_STORE/user"))
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(400, response.statusCode());
        assertNotNull(response.body());
    }

    @Test
    void shouldGetByKeyInStringStringStore() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(
                        "http://localhost:8083/store/key-value/timestamped/STRING_STRING_TIMESTAMPED_STORE/user"))
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        StateStoreRecord body = objectMapper.readValue(response.body(), StateStoreRecord.class);

        assertEquals(200, response.statusCode());
        assertEquals("user", body.getKey());
        assertEquals("Doe", body.getValue());
        assertNotNull(body.getTimestamp());
    }

    @Test
    void shouldGetByKeyInStringAvroStore() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8083/store/key-value/timestamped/STRING_AVRO_TIMESTAMPED_STORE/user"))
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        StateStoreRecord body = objectMapper.readValue(response.body(), StateStoreRecord.class);

        assertEquals(200, response.statusCode());
        assertEquals("user", body.getKey());
        assertEquals(1, ((Map<?, ?>) body.getValue()).get("id"));
        assertEquals("John", ((Map<?, ?>) body.getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) body.getValue()).get("lastName"));
        assertEquals("2000-01-01T01:00:00Z", ((Map<?, ?>) body.getValue()).get("birthDate"));
        assertNotNull(body.getTimestamp());
    }

    @ParameterizedTest
    @CsvSource({
        "http://localhost:8083/store/key-value/timestamped/STRING_STRING_TIMESTAMPED_STORE",
        "http://localhost:8083/store/key-value/timestamped/local/STRING_STRING_TIMESTAMPED_STORE"
    })
    void shouldGetAllInStringStringStore(String url) throws IOException, InterruptedException {
        HttpRequest request =
                HttpRequest.newBuilder().uri(URI.create(url)).GET().build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        List<StateStoreRecord> body = objectMapper.readValue(response.body(), new TypeReference<>() {});

        assertEquals(200, response.statusCode());
        assertEquals("user", body.get(0).getKey());
        assertEquals("Doe", body.get(0).getValue());
        assertNotNull(body.get(0).getTimestamp());
    }

    @ParameterizedTest
    @CsvSource({
        "http://localhost:8083/store/key-value/timestamped/STRING_AVRO_TIMESTAMPED_STORE",
        "http://localhost:8083/store/key-value/timestamped/local/STRING_AVRO_TIMESTAMPED_STORE"
    })
    void shouldGetFromStringAvroStores(String url) throws IOException, InterruptedException {
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
        assertNotNull(body.get(0).getTimestamp());
    }

    @Test
    void shouldGetByKeyInStringAvroStoreFromService() {
        StateStoreRecord stateStoreRecord =
                timestampedKeyValueService.getByKey("STRING_AVRO_TIMESTAMPED_STORE", "user");

        assertEquals("user", stateStoreRecord.getKey());
        assertEquals(1L, ((Map<?, ?>) stateStoreRecord.getValue()).get("id"));
        assertEquals("John", ((Map<?, ?>) stateStoreRecord.getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) stateStoreRecord.getValue()).get("lastName"));
        assertEquals("2000-01-01T01:00:00Z", ((Map<?, ?>) stateStoreRecord.getValue()).get("birthDate"));
        assertNotNull(stateStoreRecord.getTimestamp());
    }

    @Test
    void shouldGetAllInStringAvroStoreFromService() {
        List<StateStoreRecord> stateQueryData = timestampedKeyValueService.getAll("STRING_AVRO_TIMESTAMPED_STORE");

        assertEquals("user", stateQueryData.get(0).getKey());
        assertEquals(1L, ((Map<?, ?>) stateQueryData.get(0).getValue()).get("id"));
        assertEquals("John", ((Map<?, ?>) stateQueryData.get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) stateQueryData.get(0).getValue()).get("lastName"));
        assertEquals("2000-01-01T01:00:00Z", ((Map<?, ?>) stateQueryData.get(0).getValue()).get("birthDate"));
        assertNotNull(stateQueryData.get(0).getTimestamp());
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
                            StoreBuilder<TimestampedKeyValueStore<String, String>> stringStringKeyValueStoreBuilder =
                                    Stores.timestampedKeyValueStoreBuilder(
                                            Stores.persistentTimestampedKeyValueStore(
                                                    "STRING_STRING_TIMESTAMPED_STORE"),
                                            Serdes.String(),
                                            Serdes.String());

                            return Set.of(stringStringKeyValueStoreBuilder);
                        }

                        @Override
                        public Processor<String, String, String, String> get() {
                            return new Processor<>() {
                                private TimestampedKeyValueStore<String, String> stringStringKeyValueStore;

                                @Override
                                public void init(ProcessorContext<String, String> context) {
                                    this.stringStringKeyValueStore =
                                            context.getStateStore("STRING_STRING_TIMESTAMPED_STORE");
                                }

                                @Override
                                public void process(Record<String, String> message) {
                                    stringStringKeyValueStore.put(
                                            message.key(),
                                            ValueAndTimestamp.make(message.value(), message.timestamp()));
                                }
                            };
                        }
                    });

            streamsBuilder.stream(
                            "AVRO_TOPIC", Consumed.with(Serdes.String(), SerdesUtils.<KafkaUserStub>getValueSerdes()))
                    .process(new ProcessorSupplier<String, KafkaUserStub, String, KafkaUserStub>() {
                        @Override
                        public Set<StoreBuilder<?>> stores() {
                            StoreBuilder<TimestampedKeyValueStore<String, KafkaUserStub>>
                                    stringAvroKeyValueStoreBuilder = Stores.timestampedKeyValueStoreBuilder(
                                            Stores.persistentTimestampedKeyValueStore("STRING_AVRO_TIMESTAMPED_STORE"),
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
                                private TimestampedKeyValueStore<String, KafkaUserStub> stringAvroKeyValueStore;
                                private WindowStore<String, KafkaUserStub> stringAvroWindowStore;

                                @Override
                                public void init(ProcessorContext<String, KafkaUserStub> context) {
                                    this.stringAvroKeyValueStore =
                                            context.getStateStore("STRING_AVRO_TIMESTAMPED_STORE");

                                    this.stringAvroWindowStore = context.getStateStore("STRING_AVRO_WINDOW_STORE");
                                }

                                @Override
                                public void process(Record<String, KafkaUserStub> message) {
                                    stringAvroKeyValueStore.put(
                                            message.key(),
                                            ValueAndTimestamp.make(message.value(), message.timestamp()));
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
