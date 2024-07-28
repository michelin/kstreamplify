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
import com.michelin.kstreamplify.serde.SerdesUtils;
import com.michelin.kstreamplify.service.InteractiveQueriesService;
import com.michelin.kstreamplify.store.StateStoreHostInfo;
import com.michelin.kstreamplify.store.StateStoreRecord;
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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Repartitioned;
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
        createTopics(broker.getBootstrapServers(), "INPUT_TOPIC");

        KafkaPersonStub kafkaPersonStub = KafkaPersonStub.newBuilder()
            .setId(1L)
            .setFirstName("John")
            .setLastName("Doe")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .build();

        try (KafkaProducer<String, KafkaPersonStub> avroKafkaProducer = new KafkaProducer<>(
            Map.of(BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers(),
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName(),
                SCHEMA_REGISTRY_URL_CONFIG, "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getFirstMappedPort()))) {

            ProducerRecord<String, KafkaPersonStub> message = new ProducerRecord<>(
                "INPUT_TOPIC", "person", kafkaPersonStub);

            avroKafkaProducer
                .send(message)
                .get();
        }
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        waitingForKafkaStreamsToStart();
        waitingForLocalStoreToReachOffset(Map.of(
            "STRING_STRING_STORE", Map.of(0, 1L),
            "STRING_AVRO_STORE", Map.of(0, 1L),
            "STRING_AVRO_TIMESTAMPED_STORE", Map.of(0, 1L),
            "STRING_AVRO_WINDOW_STORE", Map.of(0, 1L),
            "STRING_AVRO_TIMESTAMPED_WINDOW_STORE", Map.of(0, 1L)
        ));
    }

    @Test
    void shouldGetStoresAndHosts() {
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
        ResponseEntity<List<StateStoreHostInfo>> hosts = restTemplate
            .exchange("http://localhost:8085/store/STRING_STRING_STORE/info", GET, null, new ParameterizedTypeReference<>() {
            });

        assertEquals(200, hosts.getStatusCode().value());
        assertNotNull(hosts.getBody());
        assertEquals("localhost", hosts.getBody().get(0).host());
        assertEquals(8085, hosts.getBody().get(0).port());
    }

    @Test
    void shouldGetByKeyWrongStore() {
        ResponseEntity<String> response = restTemplate
            .getForEntity("http://localhost:8085/store/key-value/WRONG_STORE/person", String.class);

        assertEquals(404, response.getStatusCode().value());
        assertEquals("State store WRONG_STORE not found", response.getBody());
    }

    @Test
    void shouldGetByKeyWrongKey() {
        ResponseEntity<String> response = restTemplate
            .getForEntity("http://localhost:8085/store/key-value/STRING_STRING_STORE/wrongKey", String.class);

        assertEquals(404, response.getStatusCode().value());
        assertEquals("Key wrongKey not found", response.getBody());
    }

    @Test
    void shouldGetByKeyWrongStoreType() {
        ResponseEntity<String> response = restTemplate
            .getForEntity("http://localhost:8085/store/key-value/STRING_AVRO_WINDOW_STORE/person", String.class);

        assertEquals(400, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().contains("Cannot get result for failed query."));
    }

    @Test
    void shouldGetByKeyInStringStringKeyValueStore() {
        ResponseEntity<StateStoreRecord> response = restTemplate
            .getForEntity("http://localhost:8085/store/key-value/STRING_STRING_STORE/person", StateStoreRecord.class);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("Doe", response.getBody().getValue());
        assertNull(response.getBody().getTimestamp());
    }

    @Test
    void shouldGetByKeyInStringAvroKeyValueStore() {
        ResponseEntity<StateStoreRecord> response = restTemplate
            .getForEntity("http://localhost:8085/store/key-value/STRING_AVRO_STORE/person?includeKey=true&includeMetadata=true",
                StateStoreRecord.class);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("person", response.getBody().getKey());
        assertEquals("John", ((HashMap<?, ?>) response.getBody().getValue()).get("firstName"));
        assertEquals("Doe", ((HashMap<?, ?>) response.getBody().getValue()).get("lastName"));
        assertNull(response.getBody().getTimestamp());
        assertEquals("localhost", response.getBody().getHostInfo().host());
        assertEquals(8085, response.getBody().getHostInfo().port());
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
        assertEquals("localhost", stateStoreRecord.getHostInfo().host());
        assertEquals(8085, stateStoreRecord.getHostInfo().port());
    }

    @Test
    void shouldGetByKeyInStringAvroTimestampedKeyValueStore() {
        ResponseEntity<StateStoreRecord> response = restTemplate
            .getForEntity("http://localhost:8085/store/key-value/STRING_AVRO_TIMESTAMPED_STORE/person?includeKey=true&includeMetadata=true",
                StateStoreRecord.class);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("person", response.getBody().getKey());
        assertEquals("John", ((HashMap<?, ?>) response.getBody().getValue()).get("firstName"));
        assertEquals("Doe", ((HashMap<?, ?>) response.getBody().getValue()).get("lastName"));
        assertNotNull(response.getBody().getTimestamp());
        assertEquals("localhost", response.getBody().getHostInfo().host());
        assertEquals(8085, response.getBody().getHostInfo().port());
    }

    @Test
    void shouldGetAllWrongStore() {
        ResponseEntity<String> response = restTemplate
            .getForEntity("http://localhost:8085/store/key-value/WRONG_STORE", String.class);

        assertEquals(404, response.getStatusCode().value());
        assertEquals("State store WRONG_STORE not found", response.getBody());
    }

    @Test
    void shouldGetAllInStringStringKeyValueStore() {
        ResponseEntity<List<StateStoreRecord>> response = restTemplate
            .exchange("http://localhost:8085/store/key-value/STRING_STRING_STORE", GET, null, new ParameterizedTypeReference<>() {
            });

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("Doe", response.getBody().get(0).getValue());
        assertNull(response.getBody().get(0).getTimestamp());
    }

    @Test
    void shouldGetAllInStringAvroKeyValueStore() {
        ResponseEntity<List<StateStoreRecord>> response = restTemplate
            .exchange("http://localhost:8085/store/key-value/STRING_AVRO_STORE?includeKey=true&includeMetadata=true", GET, null, new ParameterizedTypeReference<>() {
            });

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("person", response.getBody().get(0).getKey());
        assertEquals(1, ((Map<?, ?>) response.getBody().get(0).getValue()).get("id"));
        assertEquals("John", ((Map<?, ?>) response.getBody().get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) response.getBody().get(0).getValue()).get("lastName"));
        assertEquals("2000-01-01T01:00:00Z", ((Map<?, ?>) response.getBody().get(0).getValue()).get("birthDate"));
        assertNull(response.getBody().get(0).getTimestamp());
        assertEquals("localhost", response.getBody().get(0).getHostInfo().host());
        assertEquals(8085, response.getBody().get(0).getHostInfo().port());
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
        assertEquals("localhost", stateQueryData.get(0).getHostInfo().host());
        assertEquals(8085, stateQueryData.get(0).getHostInfo().port());
    }

    @Test
    void shouldGetAllInStringAvroTimestampedKeyValueStore() {
        ResponseEntity<List<StateStoreRecord>> response = restTemplate
            .exchange("http://localhost:8085/store/key-value/STRING_AVRO_TIMESTAMPED_STORE?includeKey=true&includeMetadata=true", GET, null, new ParameterizedTypeReference<>() {
            });

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("person", response.getBody().get(0).getKey());
        assertEquals(1, ((Map<?, ?>) response.getBody().get(0).getValue()).get("id"));
        assertEquals("John", ((Map<?, ?>) response.getBody().get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) response.getBody().get(0).getValue()).get("lastName"));
        assertEquals("2000-01-01T01:00:00Z", ((Map<?, ?>) response.getBody().get(0).getValue()).get("birthDate"));
        assertNotNull(response.getBody().get(0).getTimestamp());
        assertEquals("localhost", response.getBody().get(0).getHostInfo().host());
        assertEquals(8085, response.getBody().get(0).getHostInfo().port());
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
            KStream<String, KafkaPersonStub> stream = streamsBuilder
                .stream("INPUT_TOPIC", Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()));

            stream
                .process(new ProcessorSupplier<String, KafkaPersonStub, String, KafkaPersonStub>() {
                    @Override
                    public Set<StoreBuilder<?>> stores() {
                        // <String, String> key-value store
                        StoreBuilder<KeyValueStore<String, String>> stringStringKeyValueStoreBuilder = Stores
                            .keyValueStoreBuilder(
                                Stores.persistentKeyValueStore("STRING_STRING_STORE"),
                                Serdes.String(), Serdes.String());

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
                            stringStringKeyValueStoreBuilder,
                            stringAvroKeyValueStoreBuilder,
                            stringAvroTimestampedKeyValueStoreBuilder,
                            stringAvroWindowStoreBuilder,
                            stringAvroTimestampedWindowStoreBuilder
                        );
                    }

                    @Override
                    public Processor<String, KafkaPersonStub, String, KafkaPersonStub> get() {
                        return new Processor<>() {
                            private KeyValueStore<String, String> stringStringKeyValueStore;
                            private KeyValueStore<String, KafkaPersonStub> stringAvroKeyValueStore;
                            private TimestampedKeyValueStore<String, KafkaPersonStub>
                                stringAvroTimestampedKeyValueStore;
                            private WindowStore<String, KafkaPersonStub> stringAvroWindowStore;
                            private TimestampedWindowStore<String, KafkaPersonStub> stringAvroTimestampedWindowStore;

                            @Override
                            public void init(ProcessorContext<String, KafkaPersonStub> context) {
                                this.stringStringKeyValueStore = context
                                    .getStateStore("STRING_STRING_STORE");

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
                                stringStringKeyValueStore.put(message.key(), message.value().getLastName());
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
