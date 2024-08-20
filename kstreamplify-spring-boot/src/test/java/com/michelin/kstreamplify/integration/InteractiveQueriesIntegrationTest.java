package com.michelin.kstreamplify.integration;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.DEFINED_PORT;
import static org.springframework.http.HttpMethod.GET;

import com.michelin.kstreamplify.avro.KafkaPersonStub;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.integration.container.KafkaIntegrationTest;
import com.michelin.kstreamplify.serde.SerdesUtils;
import com.michelin.kstreamplify.service.InteractiveQueriesService;
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
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
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
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
@ActiveProfiles("interactive-queries")
@SpringBootTest(webEnvironment = DEFINED_PORT)
class InteractiveQueriesIntegrationTest extends KafkaIntegrationTest {
    @Autowired
    private InteractiveQueriesService interactiveQueriesService;

    @BeforeAll
    static void globalSetUp() throws ExecutionException, InterruptedException {
        createTopics(
            broker.getBootstrapServers(),
            new TopicPartition("STRING_TOPIC", 3),
            new TopicPartition("AVRO_TOPIC", 2)
        );

        try (KafkaProducer<String, String> stringKafkaProducer = new KafkaProducer<>(
            Map.of(BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers(),
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {

            ProducerRecord<String, String> message = new ProducerRecord<>(
                "STRING_TOPIC", "person", "Doe");

            stringKafkaProducer
                .send(message)
                .get();
        }

        try (KafkaProducer<String, KafkaPersonStub> avroKafkaProducer = new KafkaProducer<>(
            Map.of(BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers(),
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName(),
                SCHEMA_REGISTRY_URL_CONFIG, "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getFirstMappedPort()))) {

            KafkaPersonStub kafkaPersonStub = KafkaPersonStub.newBuilder()
                .setId(1L)
                .setFirstName("John")
                .setLastName("Doe")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
                .build();

            ProducerRecord<String, KafkaPersonStub> message = new ProducerRecord<>(
                "AVRO_TOPIC", "person", kafkaPersonStub);

            avroKafkaProducer
                .send(message)
                .get();
        }
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        waitingForKafkaStreamsToStart();
        waitingForLocalStoreToReachOffset(Map.of(
            "STRING_STRING_STORE", Map.of(1, 1L),
            "STRING_AVRO_STORE", Map.of(0, 1L),
            "STRING_AVRO_TIMESTAMPED_STORE", Map.of(0, 1L),
            "STRING_AVRO_WINDOW_STORE", Map.of(0, 1L),
            "STRING_AVRO_TIMESTAMPED_WINDOW_STORE", Map.of(0, 1L)
        ));
    }

    @Test
    void shouldGetStoresAndStoreMetadata() {
        // Get stores
        ResponseEntity<List<String>> stores = restTemplate
            .exchange("http://localhost:8085/store", GET, null, new ParameterizedTypeReference<>() {
            });

        assertEquals(200, stores.getStatusCode().value());
        assertNotNull(stores.getBody());
        assertTrue(stores.getBody().containsAll(List.of(
            "STRING_STRING_STORE",
            "STRING_AVRO_STORE",
            "STRING_AVRO_TIMESTAMPED_STORE",
            "STRING_AVRO_WINDOW_STORE",
            "STRING_AVRO_TIMESTAMPED_WINDOW_STORE"
        )));

        // Get hosts
        ResponseEntity<List<StreamsMetadata>> streamsMetadata = restTemplate
            .exchange("http://localhost:8085/store/metadata/STRING_STRING_STORE", GET, null, new ParameterizedTypeReference<>() {
            });

        assertEquals(200, streamsMetadata.getStatusCode().value());
        assertNotNull(streamsMetadata.getBody());
        assertEquals(Set.of(
            "STRING_STRING_STORE",
            "STRING_AVRO_STORE",
            "STRING_AVRO_TIMESTAMPED_STORE",
            "STRING_AVRO_WINDOW_STORE",
            "STRING_AVRO_TIMESTAMPED_WINDOW_STORE"), streamsMetadata.getBody().get(0).getStateStoreNames());
        assertEquals("localhost", streamsMetadata.getBody().get(0).getHostInfo().host());
        assertEquals(8085, streamsMetadata.getBody().get(0).getHostInfo().port());
        assertEquals(Set.of(
            "AVRO_TOPIC-0",
            "AVRO_TOPIC-1",
            "STRING_TOPIC-0",
            "STRING_TOPIC-1",
            "STRING_TOPIC-2"),
            streamsMetadata.getBody().get(0).getTopicPartitions());
    }

    @ParameterizedTest
    @CsvSource({
        "http://localhost:8085/store/WRONG_STORE/person,State store WRONG_STORE not found",
        "http://localhost:8085/store/STRING_STRING_STORE/wrongKey,Key wrongKey not found",
        "http://localhost:8085/store/WRONG_STORE,State store WRONG_STORE not found",
    })
    void shouldGetErrorWhenWrongKeyOrStore(String url, String message) {
        ResponseEntity<String> response = restTemplate
            .getForEntity(url, String.class);

        assertEquals(404, response.getStatusCode().value());
        assertEquals(message, response.getBody());
    }

    @Test
    void shouldGetByKeyWrongStoreType() {
        ResponseEntity<String> response = restTemplate
            .getForEntity("http://localhost:8085/store/STRING_AVRO_WINDOW_STORE/person", String.class);

        assertEquals(400, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().contains("Cannot get result for failed query."));
    }

    @Test
    void shouldGetByKeyInStringStringKeyValueStore() {
        ResponseEntity<StateStoreRecord> response = restTemplate
            .getForEntity("http://localhost:8085/store/STRING_STRING_STORE/person", StateStoreRecord.class);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("person", response.getBody().getKey());
        assertEquals("Doe", response.getBody().getValue());
        assertNull(response.getBody().getTimestamp());
    }

    @Test
    void shouldGetByKeyInStringAvroKeyValueStore() {
        ResponseEntity<StateStoreRecord> response = restTemplate
            .getForEntity("http://localhost:8085/store/STRING_AVRO_STORE/person",
                StateStoreRecord.class);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("person", response.getBody().getKey());
        assertEquals(1, ((HashMap<?, ?>) response.getBody().getValue()).get("id"));
        assertEquals("John", ((HashMap<?, ?>) response.getBody().getValue()).get("firstName"));
        assertEquals("Doe", ((HashMap<?, ?>) response.getBody().getValue()).get("lastName"));
        assertEquals("2000-01-01T01:00:00Z", ((HashMap<?, ?>) response.getBody().getValue()).get("birthDate"));
        assertNull(response.getBody().getTimestamp());
    }

    @Test
    void shouldGetByKeyInStringAvroKeyValueStoreFromInteractiveQueriesService() {
        StateStoreRecord stateStoreRecord = interactiveQueriesService.getByKey("STRING_AVRO_STORE", "person");

        assertEquals("person", stateStoreRecord.getKey());
        assertEquals(1L, ((Map<?, ?>) stateStoreRecord.getValue()).get("id"));
        assertEquals("John", ((Map<?, ?>) stateStoreRecord.getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) stateStoreRecord.getValue()).get("lastName"));
        assertEquals("2000-01-01T01:00:00Z", ((Map<?, ?>) stateStoreRecord.getValue()).get("birthDate"));
        assertNull(stateStoreRecord.getTimestamp());
    }

    @Test
    void shouldGetByKeyInStringAvroTimestampedKeyValueStore() {
        ResponseEntity<StateStoreRecord> response = restTemplate
            .getForEntity("http://localhost:8085/store/STRING_AVRO_TIMESTAMPED_STORE/person",
                StateStoreRecord.class);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("person", response.getBody().getKey());
        assertEquals(1, ((HashMap<?, ?>) response.getBody().getValue()).get("id"));
        assertEquals("John", ((HashMap<?, ?>) response.getBody().getValue()).get("firstName"));
        assertEquals("Doe", ((HashMap<?, ?>) response.getBody().getValue()).get("lastName"));
        assertEquals("2000-01-01T01:00:00Z", ((HashMap<?, ?>) response.getBody().getValue()).get("birthDate"));
        assertNotNull(response.getBody().getTimestamp());
    }

    @Test
    void shouldGetAllInStringStringKeyValueStore() {
        ResponseEntity<List<StateStoreRecord>> response = restTemplate
            .exchange("http://localhost:8085/store/STRING_STRING_STORE", GET, null, new ParameterizedTypeReference<>() {
            });

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("person", response.getBody().get(0).getKey());
        assertEquals("Doe", response.getBody().get(0).getValue());
        assertNull(response.getBody().get(0).getTimestamp());
    }

    @Test
    void shouldGetAllInStringAvroKeyValueStore() {
        ResponseEntity<List<StateStoreRecord>> response = restTemplate
            .exchange("http://localhost:8085/store/STRING_AVRO_STORE", GET, null, new ParameterizedTypeReference<>() {
            });

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("person", response.getBody().get(0).getKey());
        assertEquals(1, ((Map<?, ?>) response.getBody().get(0).getValue()).get("id"));
        assertEquals("John", ((Map<?, ?>) response.getBody().get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) response.getBody().get(0).getValue()).get("lastName"));
        assertEquals("2000-01-01T01:00:00Z", ((Map<?, ?>) response.getBody().get(0).getValue()).get("birthDate"));
        assertNull(response.getBody().get(0).getTimestamp());
    }

    @Test
    void shouldGetAllInStringAvroKeyValueStoreFromInteractiveQueriesService() {
        List<StateStoreRecord> stateQueryData = interactiveQueriesService.getAll("STRING_AVRO_STORE");

        assertEquals("person", stateQueryData.get(0).getKey());
        assertEquals(1L, ((Map<?, ?>) stateQueryData.get(0).getValue()).get("id"));
        assertEquals("John", ((Map<?, ?>) stateQueryData.get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) stateQueryData.get(0).getValue()).get("lastName"));
        assertEquals("2000-01-01T01:00:00Z", ((Map<?, ?>) stateQueryData.get(0).getValue()).get("birthDate"));
        assertNull(stateQueryData.get(0).getTimestamp());
    }

    @Test
    void shouldGetAllInStringAvroTimestampedKeyValueStore() {
        ResponseEntity<List<StateStoreRecord>> response = restTemplate
            .exchange("http://localhost:8085/store/STRING_AVRO_TIMESTAMPED_STORE", GET, null, new ParameterizedTypeReference<>() {
            });

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("person", response.getBody().get(0).getKey());
        assertEquals(1, ((Map<?, ?>) response.getBody().get(0).getValue()).get("id"));
        assertEquals("John", ((Map<?, ?>) response.getBody().get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) response.getBody().get(0).getValue()).get("lastName"));
        assertEquals("2000-01-01T01:00:00Z", ((Map<?, ?>) response.getBody().get(0).getValue()).get("birthDate"));
        assertNotNull(response.getBody().get(0).getTimestamp());
    }

    @Test
    void shouldGetAllOnLocalhostInStringStringKeyValueStore() {
        ResponseEntity<List<StateStoreRecord>> response = restTemplate
            .exchange("http://localhost:8085/store/local/STRING_STRING_STORE", GET, null, new ParameterizedTypeReference<>() {
            });

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("person", response.getBody().get(0).getKey());
        assertEquals("Doe", response.getBody().get(0).getValue());
        assertNull(response.getBody().get(0).getTimestamp());
    }

    /**
     * Kafka Streams starter implementation for integration tests.
     * The topology consumes events from multiple topics and stores them in dedicated stores
     * so that they can be queried.
     */
    @Slf4j
    @SpringBootApplication
    static class KafkaStreamsStarterStub extends KafkaStreamsStarter {
        public static void main(String[] args) {
            SpringApplication.run(KafkaStreamsStarterStub.class, args);
        }

        @Override
        public void topology(StreamsBuilder streamsBuilder) {
            streamsBuilder
                .stream("STRING_TOPIC", Consumed.with(Serdes.String(), Serdes.String()))
                .process(new ProcessorSupplier<String, String, String, String>() {
                    @Override
                    public Set<StoreBuilder<?>> stores() {
                        // <String, String> key-value store
                        StoreBuilder<KeyValueStore<String, String>> stringStringKeyValueStoreBuilder = Stores
                            .keyValueStoreBuilder(
                                Stores.persistentKeyValueStore("STRING_STRING_STORE"),
                                Serdes.String(), Serdes.String());

                        return Set.of(
                            stringStringKeyValueStoreBuilder
                        );
                    }

                    @Override
                    public Processor<String, String, String, String> get() {
                        return new Processor<>() {
                            private KeyValueStore<String, String> stringStringKeyValueStore;

                            @Override
                            public void init(ProcessorContext<String, String> context) {
                                this.stringStringKeyValueStore = context.getStateStore("STRING_STRING_STORE");
                            }

                            @Override
                            public void process(Record<String, String> message) {
                                stringStringKeyValueStore.put(message.key(), message.value());
                            }
                        };
                    }
                });

            streamsBuilder
                .stream("AVRO_TOPIC", Consumed.with(Serdes.String(), SerdesUtils.<KafkaPersonStub>getValueSerdes()))
                .process(new ProcessorSupplier<String, KafkaPersonStub, String, KafkaPersonStub>() {
                    @Override
                    public Set<StoreBuilder<?>> stores() {
                        // <String, Avro> key-value store
                        StoreBuilder<KeyValueStore<String, KafkaPersonStub>> stringAvroKeyValueStoreBuilder = Stores
                            .keyValueStoreBuilder(
                                Stores.persistentKeyValueStore("STRING_AVRO_STORE"),
                                Serdes.String(), SerdesUtils.getValueSerdes());

                        // <String, Avro> timestamped key-value store
                        StoreBuilder<TimestampedKeyValueStore<String,
                            KafkaPersonStub>> stringAvroTimestampedKeyValueStoreBuilder = Stores
                            .timestampedKeyValueStoreBuilder(
                                Stores.persistentTimestampedKeyValueStore("STRING_AVRO_TIMESTAMPED_STORE"),
                                Serdes.String(), SerdesUtils.getValueSerdes());

                        // <String, Avro> window store
                        StoreBuilder<WindowStore<String, KafkaPersonStub>> stringAvroWindowStoreBuilder =
                            Stores.windowStoreBuilder(
                                Stores.persistentWindowStore("STRING_AVRO_WINDOW_STORE",
                                    Duration.ofMinutes(5), Duration.ofMinutes(1), false),
                                Serdes.String(), SerdesUtils.getValueSerdes());

                        // <String, Avro> timestamped window store
                        StoreBuilder<TimestampedWindowStore<String, KafkaPersonStub>>
                            stringAvroTimestampedWindowStoreBuilder = Stores.timestampedWindowStoreBuilder(
                            Stores.persistentTimestampedWindowStore("STRING_AVRO_TIMESTAMPED_WINDOW_STORE",
                                Duration.ofMinutes(5), Duration.ofMinutes(1), false),
                            Serdes.String(), SerdesUtils.getValueSerdes());

                        return Set.of(
                            stringAvroKeyValueStoreBuilder,
                            stringAvroTimestampedKeyValueStoreBuilder,
                            stringAvroWindowStoreBuilder,
                            stringAvroTimestampedWindowStoreBuilder
                        );
                    }

                    @Override
                    public Processor<String, KafkaPersonStub, String, KafkaPersonStub> get() {
                        return new Processor<>() {
                            private KeyValueStore<String, KafkaPersonStub> stringAvroKeyValueStore;
                            private TimestampedKeyValueStore<String, KafkaPersonStub>
                                stringAvroTimestampedKeyValueStore;
                            private WindowStore<String, KafkaPersonStub> stringAvroWindowStore;
                            private TimestampedWindowStore<String, KafkaPersonStub> stringAvroTimestampedWindowStore;

                            @Override
                            public void init(ProcessorContext<String, KafkaPersonStub> context) {
                                this.stringAvroKeyValueStore = context
                                    .getStateStore("STRING_AVRO_STORE");

                                this.stringAvroTimestampedKeyValueStore = context
                                    .getStateStore("STRING_AVRO_TIMESTAMPED_STORE");

                                this.stringAvroWindowStore = context
                                    .getStateStore("STRING_AVRO_WINDOW_STORE");

                                this.stringAvroTimestampedWindowStore = context
                                    .getStateStore("STRING_AVRO_TIMESTAMPED_WINDOW_STORE");
                            }

                            @Override
                            public void process(Record<String, KafkaPersonStub> message) {
                                stringAvroKeyValueStore.put(message.key(), message.value());
                                stringAvroTimestampedKeyValueStore.put(message.key(),
                                    ValueAndTimestamp.make(message.value(), message.timestamp()));
                                stringAvroWindowStore.put(message.key(), message.value(), message.timestamp());
                                stringAvroTimestampedWindowStore.put(message.key(),
                                    ValueAndTimestamp.make(message.value(), message.timestamp()),
                                    message.timestamp());

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
