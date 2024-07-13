package com.michelin.kstreamplify.integration;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.DEFINED_PORT;
import static org.springframework.http.HttpMethod.GET;

import com.michelin.kstreamplify.avro.CountryCode;
import com.michelin.kstreamplify.avro.KafkaPersonStub;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.serde.SerdesUtils;
import com.michelin.kstreamplify.service.InteractiveQueriesService;
import com.michelin.kstreamplify.store.HostInfoResponse;
import com.michelin.kstreamplify.store.StateQueryData;
import com.michelin.kstreamplify.store.StateQueryResponse;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Instant;
import java.util.Collections;
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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
@ActiveProfiles("interactive-queries")
@SpringBootTest(webEnvironment = DEFINED_PORT)
class InteractiveQueriesIntegrationTest extends KafkaIntegrationTest {
    @Autowired
    private InteractiveQueriesService interactiveQueriesService;

    @Container
    static KafkaContainer broker = new KafkaContainer(DockerImageName
        .parse("confluentinc/cp-kafka:" + CONFLUENT_PLATFORM_VERSION))
        .withNetwork(NETWORK)
        .withNetworkAliases("broker")
        .withKraft();

    @Container
    static GenericContainer<?> schemaRegistry = new GenericContainer<>(DockerImageName
        .parse("confluentinc/cp-schema-registry:" + CONFLUENT_PLATFORM_VERSION))
        .dependsOn(broker)
        .withNetwork(NETWORK)
        .withNetworkAliases("schema-registry")
        .withExposedPorts(8081)
        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://broker:9092")
        .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("kafka.properties." + BOOTSTRAP_SERVERS_CONFIG, broker::getBootstrapServers);
        registry.add("kafka.properties." + SCHEMA_REGISTRY_URL_CONFIG,
            () -> "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getFirstMappedPort());
    }

    @BeforeAll
    static void globalSetUp() throws ExecutionException, InterruptedException {
        createTopics(broker.getBootstrapServers(),
            "STRING_TOPIC", "JAVA_TOPIC", "AVRO_TOPIC");

        try (KafkaProducer<String, String> stringKafkaProducer = new KafkaProducer<>(
            Map.of(BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers(),
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
            stringKafkaProducer
                .send(new ProducerRecord<>("STRING_TOPIC", "key", "value"))
                .get();
        }

        try (KafkaProducer<String, KafkaPersonStub> avroKafkaProducer = new KafkaProducer<>(
            Map.of(BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers(),
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName(),
                SCHEMA_REGISTRY_URL_CONFIG, "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getFirstMappedPort()))) {
            avroKafkaProducer
                .send(new ProducerRecord<>("AVRO_TOPIC", "person", KafkaPersonStub.newBuilder()
                    .setId(1L)
                    .setFirstName("John")
                    .setLastName("Doe")
                    .setNationality(CountryCode.FR)
                    .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                    .build()))
                .get();
        }

        try (KafkaProducer<KafkaPersonStub, KafkaPersonStub> avroKeyValueKafkaProducer = new KafkaProducer<>(
            Map.of(BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers(),
                KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName(),
                SCHEMA_REGISTRY_URL_CONFIG, "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getFirstMappedPort()))) {
            KafkaPersonStub kafkaPersonStub = KafkaPersonStub.newBuilder()
                .setId(1L)
                .setFirstName("John")
                .setLastName("Doe")
                .setNationality(CountryCode.FR)
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build();

            avroKeyValueKafkaProducer
                .send(new ProducerRecord<>("AVRO_KEY_VALUE_TOPIC", kafkaPersonStub, kafkaPersonStub))
                .get();
        }
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        waitingForKafkaStreamsToStart();
        waitingForLocalStoreToReachOffset(Map.of(
            "STRING_STORE", Map.of(1, 1L),
            "AVRO_STORE", Map.of(0, 1L),
            "AVRO_TIMESTAMPED_STORE", Map.of(0, 1L)
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
        assertTrue(stores.getBody().containsAll(List.of("STRING_STORE", "AVRO_STORE", "AVRO_TIMESTAMPED_STORE")));

        // Get hosts
        ResponseEntity<List<HostInfoResponse>> hosts = restTemplate
            .exchange("http://localhost:8085/store/STRING_STORE/info", GET, null, new ParameterizedTypeReference<>() {
            });

        assertEquals(200, hosts.getStatusCode().value());
        assertNotNull(hosts.getBody());
        assertEquals("localhost", hosts.getBody().get(0).host());
        assertEquals(8085, hosts.getBody().get(0).port());
    }

    @Test
    void shouldGetByKey() {
        // Wrong keystore
        ResponseEntity<String> wrongStore = restTemplate
            .getForEntity("http://localhost:8085/store/WRONG_STORE/key", String.class);

        assertEquals(404, wrongStore.getStatusCode().value());
        assertEquals("State store WRONG_STORE not found", wrongStore.getBody());

        // Wrong key
        ResponseEntity<String> wrongKey = restTemplate
            .getForEntity("http://localhost:8085/store/STRING_STORE/wrongKey", String.class);

        assertEquals(404, wrongKey.getStatusCode().value());
        assertEquals("Key wrongKey not found", wrongKey.getBody());

        // Get by key
        ResponseEntity<StateQueryResponse> recordByKey = restTemplate
            .getForEntity("http://localhost:8085/store/STRING_STORE/key", StateQueryResponse.class);

        assertEquals(200, recordByKey.getStatusCode().value());
        assertNotNull(recordByKey.getBody());
        assertEquals("value", recordByKey.getBody().getValue());

        // Get by key with metadata
        ResponseEntity<StateQueryResponse> recordByKeyWithMetadata = restTemplate
            .getForEntity("http://localhost:8085/store/STRING_STORE/key?includeKey=true&includeMetadata=true", StateQueryResponse.class);

        assertEquals(200, recordByKeyWithMetadata.getStatusCode().value());
        assertNotNull(recordByKeyWithMetadata.getBody());
        assertEquals("key", recordByKeyWithMetadata.getBody().getKey());
        assertEquals("value", recordByKeyWithMetadata.getBody().getValue());
        assertNotNull(recordByKeyWithMetadata.getBody().getTimestamp());
        assertEquals("localhost", recordByKeyWithMetadata.getBody().getHostInfo().host());
        assertEquals(8085, recordByKeyWithMetadata.getBody().getHostInfo().port());
        assertEquals("STRING_TOPIC", recordByKeyWithMetadata.getBody().getPositionVectors().get(0).topic());
        assertEquals(1, recordByKeyWithMetadata.getBody().getPositionVectors().get(0).partition());
        assertNotNull(recordByKeyWithMetadata.getBody().getPositionVectors().get(0).offset());

        // Get Avro by key with metadata
        ResponseEntity<StateQueryResponse> avroRecordByKeyWithMetadata = restTemplate
            .getForEntity("http://localhost:8085/store/AVRO_STORE/person?includeKey=true&includeMetadata=true", StateQueryResponse.class);

        assertEquals(200, avroRecordByKeyWithMetadata.getStatusCode().value());
        assertNotNull(avroRecordByKeyWithMetadata.getBody());
        assertEquals("person", avroRecordByKeyWithMetadata.getBody().getKey());
        assertEquals("John", ((HashMap<?, ?>) avroRecordByKeyWithMetadata.getBody().getValue()).get("firstName"));
        assertEquals("Doe", ((HashMap<?, ?>) avroRecordByKeyWithMetadata.getBody().getValue()).get("lastName"));
        assertNotNull(avroRecordByKeyWithMetadata.getBody().getTimestamp());
        assertEquals("localhost", avroRecordByKeyWithMetadata.getBody().getHostInfo().host());
        assertEquals(8085, avroRecordByKeyWithMetadata.getBody().getHostInfo().port());
        assertEquals("AVRO_TOPIC", avroRecordByKeyWithMetadata.getBody().getPositionVectors().get(0).topic());
        assertEquals(0, avroRecordByKeyWithMetadata.getBody().getPositionVectors().get(0).partition());
        assertNotNull(avroRecordByKeyWithMetadata.getBody().getPositionVectors().get(0).offset());

        // Get Avro by key with metadata from timestamped store
        ResponseEntity<StateQueryResponse> avroTsRecordByKeyWithMetadata = restTemplate
            .getForEntity("http://localhost:8085/store/AVRO_TIMESTAMPED_STORE/person?includeKey=true&includeMetadata=true", StateQueryResponse.class);

        assertEquals(200, avroTsRecordByKeyWithMetadata.getStatusCode().value());
        assertNotNull(avroTsRecordByKeyWithMetadata.getBody());
        assertEquals("person", avroTsRecordByKeyWithMetadata.getBody().getKey());
        assertEquals("John", ((HashMap<?, ?>) avroTsRecordByKeyWithMetadata.getBody().getValue()).get("firstName"));
    }

    @Test
    void shouldGetAll() {
        // Wrong keystore
        ResponseEntity<String> wrongStore = restTemplate
            .getForEntity("http://localhost:8085/store/WRONG_STORE", String.class);

        assertEquals(404, wrongStore.getStatusCode().value());
        assertEquals("State store WRONG_STORE not found", wrongStore.getBody());

        // Get all
        ResponseEntity<List<StateQueryResponse>> allRecords = restTemplate
            .exchange("http://localhost:8085/store/STRING_STORE", GET, null, new ParameterizedTypeReference<>() {
            });

        assertEquals(200, allRecords.getStatusCode().value());
        assertNotNull(allRecords.getBody());
        assertEquals("value", allRecords.getBody().get(0).getValue());

        // Get all with metadata
        ResponseEntity<List<StateQueryResponse>> allRecordsMetadata = restTemplate
            .exchange("http://localhost:8085/store/STRING_STORE?includeKey=true&includeMetadata=true", GET, null, new ParameterizedTypeReference<>() {
            });

        assertEquals(200, allRecordsMetadata.getStatusCode().value());
        assertNotNull(allRecordsMetadata.getBody());
        assertEquals("key", allRecordsMetadata.getBody().get(0).getKey());
        assertEquals("value", allRecordsMetadata.getBody().get(0).getValue());
        assertNotNull(allRecordsMetadata.getBody().get(0).getTimestamp());
        assertEquals("localhost", allRecordsMetadata.getBody().get(0).getHostInfo().host());
        assertEquals(8085, allRecordsMetadata.getBody().get(0).getHostInfo().port());
        assertEquals("STRING_TOPIC", allRecordsMetadata.getBody().get(0).getPositionVectors().get(0).topic());
        assertEquals(1, allRecordsMetadata.getBody().get(0).getPositionVectors().get(0).partition());
        assertNotNull(allRecordsMetadata.getBody().get(0).getPositionVectors().get(0).offset());

        // Get all Avro with metadata
        ResponseEntity<List<StateQueryResponse>> allAvroRecordsMetadata = restTemplate
            .exchange("http://localhost:8085/store/AVRO_STORE?includeKey=true&includeMetadata=true", GET, null, new ParameterizedTypeReference<>() {
            });

        assertEquals(200, allAvroRecordsMetadata.getStatusCode().value());
        assertNotNull(allAvroRecordsMetadata.getBody());
        assertEquals("person", allAvroRecordsMetadata.getBody().get(0).getKey());
        assertEquals("John", ((HashMap<?, ?>) allAvroRecordsMetadata.getBody().get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((HashMap<?, ?>) allAvroRecordsMetadata.getBody().get(0).getValue()).get("lastName"));
        assertNotNull(allAvroRecordsMetadata.getBody().get(0).getTimestamp());
        assertEquals("localhost", allAvroRecordsMetadata.getBody().get(0).getHostInfo().host());
        assertEquals(8085, allAvroRecordsMetadata.getBody().get(0).getHostInfo().port());
        assertEquals("AVRO_TOPIC", allAvroRecordsMetadata.getBody().get(0).getPositionVectors().get(0).topic());
        assertEquals(0, allAvroRecordsMetadata.getBody().get(0).getPositionVectors().get(0).partition());
        assertNotNull(allAvroRecordsMetadata.getBody().get(0).getPositionVectors().get(0).offset());
    }

    @Test
    void shouldGetMessageFromInteractiveQueriesService() {
        KafkaPersonStub key = KafkaPersonStub.newBuilder()
            .setId(1L)
            .setFirstName("John")
            .setLastName("Doe")
            .setNationality(CountryCode.FR)
            .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
            .build();

        StateQueryData<KafkaPersonStub, KafkaPersonStub> stateQueryData = interactiveQueriesService
            .getByKey("AVRO_KEY_VALUE_STORE",
                key,
                SerdesUtils.<KafkaPersonStub>getKeySerdes().serializer(),
                KafkaPersonStub.class);

        assertEquals(key, stateQueryData.getKey());
        assertEquals(key, stateQueryData.getValue()); // Key and value are the same in this case
        assertNotNull(stateQueryData.getTimestamp());
        assertEquals("localhost", stateQueryData.getHostInfo().host());
        assertEquals(8085, stateQueryData.getHostInfo().port());
        assertEquals("AVRO_KEY_VALUE_TOPIC", stateQueryData.getPositionVectors().get(0).topic());
        assertEquals(0, stateQueryData.getPositionVectors().get(0).partition());
        assertNotNull(stateQueryData.getPositionVectors().get(0).offset());
    }

    /**
     * Kafka Streams starter implementation for integration tests.
     * The topology consumes events from multiple topics (string, Java, Avro) and stores them in dedicated stores
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
                .table("STRING_TOPIC", Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("STRING_STORE"));

            KStream<String, KafkaPersonStub> personStubStream = streamsBuilder
                .table("AVRO_TOPIC", Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()),
                    Materialized.<String, KafkaPersonStub, KeyValueStore<Bytes, byte[]>>as("AVRO_STORE"))
                .toStream();

            personStubStream
                .process(new ProcessorSupplier<String, KafkaPersonStub, String, KafkaPersonStub>() {
                    @Override
                    public Set<StoreBuilder<?>> stores() {
                        StoreBuilder<TimestampedKeyValueStore<String, KafkaPersonStub>> storeBuilder = Stores
                            .timestampedKeyValueStoreBuilder(
                                Stores.persistentTimestampedKeyValueStore("AVRO_TIMESTAMPED_STORE"),
                                Serdes.String(), SerdesUtils.getValueSerdes());
                        return Collections.singleton(storeBuilder);
                    }

                    @Override
                    public Processor<String, KafkaPersonStub, String, KafkaPersonStub> get() {
                        return new Processor<>() {
                            private TimestampedKeyValueStore<String, KafkaPersonStub> kafkaPersonStore;

                            @Override
                            public void init(ProcessorContext<String, KafkaPersonStub> context) {
                                this.kafkaPersonStore = context.getStateStore("AVRO_TIMESTAMPED_STORE");
                            }

                            @Override
                            public void process(Record<String, KafkaPersonStub> record) {
                                kafkaPersonStore.put(record.key(),
                                    ValueAndTimestamp.make(record.value(), record.timestamp()));
                            }
                        };
                    }
                });

            streamsBuilder
                .table("AVRO_KEY_VALUE_TOPIC", Consumed.with(SerdesUtils.getKeySerdes(), SerdesUtils.getValueSerdes()),
                    Materialized.<KafkaPersonStub, KafkaPersonStub,
                        KeyValueStore<Bytes, byte[]>>as("AVRO_KEY_VALUE_STORE"));
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
