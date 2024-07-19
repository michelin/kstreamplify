package com.michelin.kstreamplify.integration;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.michelin.kstreamplify.avro.CountryCode;
import com.michelin.kstreamplify.avro.KafkaPersonStub;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.serde.SerdesUtils;
import com.michelin.kstreamplify.service.InteractiveQueriesService;
import com.michelin.kstreamplify.store.HostInfoResponse;
import com.michelin.kstreamplify.store.StateQueryData;
import com.michelin.kstreamplify.store.StateQueryResponse;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
class InteractiveQueriesIntegrationTest extends KafkaIntegrationTest {
    private final InteractiveQueriesService interactiveQueriesService = new InteractiveQueriesService(initializer);

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

        initializer = new KafkaStreamInitializerStub(
            8081,
            "appInteractiveQueriesId",
            broker.getBootstrapServers(),
            "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getFirstMappedPort(),
            "/tmp/kstreamplify/kstreamplify-core-test/interactive-queries");
        initializer.init(new KafkaStreamsStarterStub());
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        waitingForKafkaStreamsToStart();
        waitingForLocalStoreToReachOffset(Map.of(
            "STRING_STORE", Map.of(1, 1L),
            "AVRO_STORE", Map.of(0, 1L),
            "AVRO_TIMESTAMPED_STORE", Map.of(0, 1L),
            "AVRO_KEY_VALUE_STORE", Map.of(0, 1L)
        ));
    }

    @Test
    void shouldGetStoresAndHosts() throws IOException, InterruptedException {
        // Get stores
        HttpRequest storesRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store"))
            .GET()
            .build();

        HttpResponse<String> storesResponse = httpClient.send(storesRequest, HttpResponse.BodyHandlers.ofString());
        List<String> stores = objectMapper.readValue(storesResponse.body(), new TypeReference<>() {});

        assertEquals(200, storesResponse.statusCode());
        assertTrue(stores.containsAll(List.of("STRING_STORE", "AVRO_STORE", "AVRO_TIMESTAMPED_STORE",
            "AVRO_KEY_VALUE_STORE")));

        // Get hosts
        HttpRequest hostsRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/STRING_STORE/info"))
            .GET()
            .build();

        HttpResponse<String> hostsResponse = httpClient.send(hostsRequest, HttpResponse.BodyHandlers.ofString());
        List<HostInfoResponse> hosts = objectMapper.readValue(hostsResponse.body(), new TypeReference<>() {});

        assertEquals(200, hostsResponse.statusCode());
        assertEquals("localhost", hosts.get(0).host());
        assertEquals(8081, hosts.get(0).port());
    }

    @Test
    void shouldGetByKey() throws IOException, InterruptedException {
        // Wrong store
        HttpRequest wrongStoreRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/WRONG_STORE/key"))
            .GET()
            .build();

        HttpResponse<String> wrongStoreResponse = httpClient.send(wrongStoreRequest,
            HttpResponse.BodyHandlers.ofString());

        assertEquals(404, wrongStoreResponse.statusCode());
        assertEquals("State store WRONG_STORE not found", wrongStoreResponse.body());

        // Wrong key
        HttpRequest wrongKeyRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/STRING_STORE/wrongKey"))
            .GET()
            .build();

        HttpResponse<String> wrongKeyResponse = httpClient.send(wrongKeyRequest,
            HttpResponse.BodyHandlers.ofString());

        assertEquals(404, wrongKeyResponse.statusCode());
        assertEquals("Key wrongKey not found", wrongKeyResponse.body());

        // Get by key from String store
        HttpRequest recordByKeyRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/STRING_STORE/key"))
            .GET()
            .build();

        HttpResponse<String> recordByKeyResponse = httpClient.send(recordByKeyRequest,
            HttpResponse.BodyHandlers.ofString());

        StateQueryResponse recordByKey = objectMapper.readValue(recordByKeyResponse.body(), StateQueryResponse.class);

        assertEquals(200, recordByKeyResponse.statusCode());
        assertEquals("value", recordByKey.getValue());

        // Get by key from Avro store with metadata
        HttpRequest avroRecordByKeyWithMetadataRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/AVRO_STORE/person?includeKey=true&includeMetadata=true"))
            .GET()
            .build();

        HttpResponse<String> avroRecordByKeyWithMetadataResponse = httpClient.send(avroRecordByKeyWithMetadataRequest,
            HttpResponse.BodyHandlers.ofString());

        StateQueryResponse avroRecordByKeyWithMetadata = objectMapper
            .readValue(avroRecordByKeyWithMetadataResponse.body(), StateQueryResponse.class);

        assertEquals(200, avroRecordByKeyWithMetadataResponse.statusCode());
        assertEquals("person", avroRecordByKeyWithMetadata.getKey());
        assertEquals("John", ((HashMap<?, ?>) avroRecordByKeyWithMetadata.getValue()).get("firstName"));
        assertEquals("Doe", ((HashMap<?, ?>) avroRecordByKeyWithMetadata.getValue()).get("lastName"));
        assertNotNull(avroRecordByKeyWithMetadata.getTimestamp());
        assertEquals("localhost", avroRecordByKeyWithMetadata.getHostInfo().host());
        assertEquals(8081, avroRecordByKeyWithMetadata.getHostInfo().port());
        assertEquals("AVRO_TOPIC", avroRecordByKeyWithMetadata.getPositionVectors().get(0).topic());
        assertEquals(0, avroRecordByKeyWithMetadata.getPositionVectors().get(0).partition());
        assertNotNull(avroRecordByKeyWithMetadata.getPositionVectors().get(0).offset());

        // Get by key from timestamped Avro store with metadata
        HttpRequest avroTsRecordByKeyWithMetadataRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/AVRO_TIMESTAMPED_STORE/person?includeKey=true&includeMetadata=true"))
            .GET()
            .build();

        HttpResponse<String> avroTsRecordByKeyWithMetadataResponse = httpClient
            .send(avroTsRecordByKeyWithMetadataRequest, HttpResponse.BodyHandlers.ofString());

        StateQueryResponse avroTsRecordByKeyWithMetadata = objectMapper
            .readValue(avroTsRecordByKeyWithMetadataResponse.body(), StateQueryResponse.class);

        assertEquals(200, avroTsRecordByKeyWithMetadataResponse.statusCode());
        assertEquals("person", avroTsRecordByKeyWithMetadata.getKey());
        assertEquals("John", ((HashMap<?, ?>) avroTsRecordByKeyWithMetadata.getValue()).get("firstName"));
    }

    @Test
    void shouldGetAll() throws IOException, InterruptedException {
        // Wrong store
        HttpRequest wrongStoreRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/WRONG_STORE/key"))
            .GET()
            .build();

        HttpResponse<String> wrongStoreResponse = httpClient.send(wrongStoreRequest,
            HttpResponse.BodyHandlers.ofString());

        assertEquals(404, wrongStoreResponse.statusCode());
        assertEquals("State store WRONG_STORE not found", wrongStoreResponse.body());

        // Get all from String store
        HttpRequest allRecordsRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/STRING_STORE"))
            .GET()
            .build();

        HttpResponse<String> allRecordsResponse = httpClient.send(allRecordsRequest,
            HttpResponse.BodyHandlers.ofString());

        List<StateQueryResponse> allRecords = objectMapper
            .readValue(allRecordsResponse.body(), new TypeReference<>() {});

        assertEquals(200, allRecordsResponse.statusCode());
        assertEquals("value", allRecords.get(0).getValue());

        // Get all from Avro store with metadata
        HttpRequest allAvroRecordsMetadataRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/AVRO_STORE?includeKey=true&includeMetadata=true"))
            .GET()
            .build();

        HttpResponse<String> allAvroRecordsMetadataResponse = httpClient.send(allAvroRecordsMetadataRequest,
            HttpResponse.BodyHandlers.ofString());

        List<StateQueryResponse> allAvroRecordsMetadata = objectMapper
            .readValue(allAvroRecordsMetadataResponse.body(), new TypeReference<>() {});

        assertEquals(200, allAvroRecordsMetadataResponse.statusCode());
        assertEquals("person", allAvroRecordsMetadata.get(0).getKey());
        assertEquals("John", ((HashMap<?, ?>) allAvroRecordsMetadata.get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((HashMap<?, ?>) allAvroRecordsMetadata.get(0).getValue()).get("lastName"));
        assertNotNull(allAvroRecordsMetadata.get(0).getTimestamp());
        assertEquals("localhost", allAvroRecordsMetadata.get(0).getHostInfo().host());
        assertEquals(8081, allAvroRecordsMetadata.get(0).getHostInfo().port());
        assertEquals("AVRO_TOPIC", allAvroRecordsMetadata.get(0).getPositionVectors().get(0).topic());
        assertEquals(0, allAvroRecordsMetadata.get(0).getPositionVectors().get(0).partition());
        assertNotNull(allAvroRecordsMetadata.get(0).getPositionVectors().get(0).offset());
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
        assertEquals(8081, stateQueryData.getHostInfo().port());
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
    static class KafkaStreamsStarterStub extends KafkaStreamsStarter {
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
                            public void process(Record<String, KafkaPersonStub> message) {
                                kafkaPersonStore.put(message.key(),
                                    ValueAndTimestamp.make(message.value(), message.timestamp()));
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
