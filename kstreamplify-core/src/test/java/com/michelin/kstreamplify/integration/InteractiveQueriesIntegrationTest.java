package com.michelin.kstreamplify.integration;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
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
        createTopics(broker.getBootstrapServers(), "INPUT_TOPIC");

        KafkaPersonStub kafkaPersonStub = KafkaPersonStub.newBuilder()
            .setId(1L)
            .setFirstName("John")
            .setLastName("Doe")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
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
            "STRING_STRING_STORE", Map.of(0, 1L),
            "STRING_AVRO_STORE", Map.of(0, 1L),
            "AVRO_AVRO_STORE", Map.of(1, 1L),
            "STRING_AVRO_TIMESTAMPED_STORE", Map.of(0, 1L),
            "STRING_AVRO_WINDOW_STORE", Map.of(0, 1L),
            "STRING_AVRO_TIMESTAMPED_WINDOW_STORE", Map.of(0, 1L)
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
        assertTrue(stores.containsAll(List.of(
            "STRING_STRING_STORE",
            "STRING_AVRO_STORE",
            "AVRO_AVRO_STORE",
            "STRING_AVRO_TIMESTAMPED_STORE",
            "STRING_AVRO_WINDOW_STORE",
            "STRING_AVRO_TIMESTAMPED_WINDOW_STORE"
        )));

        // Get hosts
        HttpRequest hostsRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/STRING_STRING_STORE/info"))
            .GET()
            .build();

        HttpResponse<String> hostsResponse = httpClient.send(hostsRequest, HttpResponse.BodyHandlers.ofString());
        List<HostInfoResponse> hosts = objectMapper.readValue(hostsResponse.body(), new TypeReference<>() {});

        assertEquals(200, hostsResponse.statusCode());
        assertEquals("localhost", hosts.get(0).host());
        assertEquals(8081, hosts.get(0).port());
    }

    @Test
    void shouldGetByKeyWrongStoreAndWrongKey() throws IOException, InterruptedException {
        HttpRequest wrongStoreRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/key-value/WRONG_STORE/person"))
            .GET()
            .build();

        HttpResponse<String> wrongStoreResponse = httpClient.send(wrongStoreRequest,
            HttpResponse.BodyHandlers.ofString());

        assertEquals(404, wrongStoreResponse.statusCode());
        assertEquals("State store WRONG_STORE not found", wrongStoreResponse.body());

        HttpRequest wrongKeyRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/key-value/STRING_STRING_STORE/wrongKey"))
            .GET()
            .build();

        HttpResponse<String> wrongKeyResponse = httpClient.send(wrongKeyRequest,
            HttpResponse.BodyHandlers.ofString());

        assertEquals(404, wrongKeyResponse.statusCode());
        assertEquals("Key wrongKey not found", wrongKeyResponse.body());
    }

    @Test
    void shouldGetByKeyInStringStringKeyValueStore() throws IOException, InterruptedException {
        HttpRequest recordByKeyRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/key-value/STRING_STRING_STORE/person"))
            .GET()
            .build();

        HttpResponse<String> recordByKeyResponse = httpClient.send(recordByKeyRequest,
            HttpResponse.BodyHandlers.ofString());

        StateQueryResponse recordByKey = objectMapper.readValue(recordByKeyResponse.body(), StateQueryResponse.class);

        assertEquals(200, recordByKeyResponse.statusCode());
        assertEquals("Doe", recordByKey.getValue());
        assertNull(recordByKey.getTimestamp());
    }

    @Test
    void shouldGetByKeyInStringAvroKeyValueStore() throws IOException, InterruptedException {
        HttpRequest avroRecordByKeyWithMetadataRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/key-value/STRING_AVRO_STORE/person?includeKey=true&includeMetadata=true"))
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
        assertNull(avroRecordByKeyWithMetadata.getTimestamp());
        assertEquals("localhost", avroRecordByKeyWithMetadata.getHostInfo().host());
        assertEquals(8081, avroRecordByKeyWithMetadata.getHostInfo().port());
        assertEquals("INPUT_TOPIC", avroRecordByKeyWithMetadata.getPositionVectors().get(0).topic());
        assertEquals(0, avroRecordByKeyWithMetadata.getPositionVectors().get(0).partition());
        assertNotNull(avroRecordByKeyWithMetadata.getPositionVectors().get(0).offset());
    }

    @Test
    void shouldGetByKeyInAvroAvroKeyValueStoreFromInteractiveQueriesService() {
        KafkaPersonStub key = KafkaPersonStub.newBuilder()
            .setId(1L)
            .setFirstName("John")
            .setLastName("Doe")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
            .build();

        StateQueryData<KafkaPersonStub, KafkaPersonStub> stateQueryData = interactiveQueriesService
            .getByKey("AVRO_AVRO_STORE",
                key,
                SerdesUtils.<KafkaPersonStub>getKeySerdes().serializer(),
                KafkaPersonStub.class);

        assertEquals(key, stateQueryData.getKey());
        assertEquals(key, stateQueryData.getValue());
        assertNull(stateQueryData.getTimestamp());
        assertEquals("localhost", stateQueryData.getHostInfo().host());
        assertEquals(8081, stateQueryData.getHostInfo().port());
        assertEquals("appInteractiveQueriesId-REPARTITIONED-repartition",
            stateQueryData.getPositionVectors().get(0).topic());
        assertEquals(1, stateQueryData.getPositionVectors().get(0).partition());
        assertNotNull(stateQueryData.getPositionVectors().get(0).offset());
    }

    @Test
    void shouldGetByKeyInStringAvroTimestampedKeyValueStore() throws IOException, InterruptedException {
        HttpRequest avroTsRecordByKeyWithMetadataRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/key-value/STRING_AVRO_TIMESTAMPED_STORE/person?includeKey=true&includeMetadata=true"))
            .GET()
            .build();

        HttpResponse<String> avroTsRecordByKeyWithMetadataResponse = httpClient
            .send(avroTsRecordByKeyWithMetadataRequest, HttpResponse.BodyHandlers.ofString());

        StateQueryResponse avroTsRecordByKeyWithMetadata = objectMapper
            .readValue(avroTsRecordByKeyWithMetadataResponse.body(), StateQueryResponse.class);

        assertEquals(200, avroTsRecordByKeyWithMetadataResponse.statusCode());
        assertEquals("person", avroTsRecordByKeyWithMetadata.getKey());
        assertEquals("John", ((HashMap<?, ?>) avroTsRecordByKeyWithMetadata.getValue()).get("firstName"));
        assertEquals("Doe", ((HashMap<?, ?>) avroTsRecordByKeyWithMetadata.getValue()).get("lastName"));
        assertNotNull(avroTsRecordByKeyWithMetadata.getTimestamp());
        assertEquals("localhost", avroTsRecordByKeyWithMetadata.getHostInfo().host());
        assertEquals(8081, avroTsRecordByKeyWithMetadata.getHostInfo().port());
        assertEquals("INPUT_TOPIC", avroTsRecordByKeyWithMetadata.getPositionVectors().get(0).topic());
        assertEquals(0, avroTsRecordByKeyWithMetadata.getPositionVectors().get(0).partition());
        assertNotNull(avroTsRecordByKeyWithMetadata.getPositionVectors().get(0).offset());
    }

    @Test
    void shouldGetAllWrongStore() throws IOException, InterruptedException {
        HttpRequest wrongStoreRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/key-value/WRONG_STORE/key"))
            .GET()
            .build();

        HttpResponse<String> wrongStoreResponse = httpClient.send(wrongStoreRequest,
            HttpResponse.BodyHandlers.ofString());

        assertEquals(404, wrongStoreResponse.statusCode());
        assertEquals("State store WRONG_STORE not found", wrongStoreResponse.body());
    }

    @Test
    void shouldGetAllInStringStringKeyValueStore() throws IOException, InterruptedException {
        HttpRequest allRecordsRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/key-value/STRING_STRING_STORE"))
            .GET()
            .build();

        HttpResponse<String> allRecordsResponse = httpClient.send(allRecordsRequest,
            HttpResponse.BodyHandlers.ofString());

        List<StateQueryResponse> allRecords = objectMapper
            .readValue(allRecordsResponse.body(), new TypeReference<>() {});

        assertEquals(200, allRecordsResponse.statusCode());
        assertEquals("Doe", allRecords.get(0).getValue());
    }

    @Test
    void shouldGetAllInStringAvroKeyValueStore() throws IOException, InterruptedException {
        HttpRequest allAvroRecordsMetadataRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/key-value/STRING_AVRO_STORE?includeKey=true&includeMetadata=true"))
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
        assertNull(allAvroRecordsMetadata.get(0).getTimestamp());
        assertEquals("localhost", allAvroRecordsMetadata.get(0).getHostInfo().host());
        assertEquals(8081, allAvroRecordsMetadata.get(0).getHostInfo().port());
        assertEquals("INPUT_TOPIC", allAvroRecordsMetadata.get(0).getPositionVectors().get(0).topic());
        assertEquals(0, allAvroRecordsMetadata.get(0).getPositionVectors().get(0).partition());
        assertNotNull(allAvroRecordsMetadata.get(0).getPositionVectors().get(0).offset());
    }

    @Test
    void shouldGetAllInAvroAvroKeyValueStore() throws IOException, InterruptedException {
        HttpRequest allAvroRecordsMetadataRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/key-value/AVRO_AVRO_STORE?includeKey=true&includeMetadata=true"))
            .GET()
            .build();

        HttpResponse<String> allAvroRecordsMetadataResponse = httpClient.send(allAvroRecordsMetadataRequest,
            HttpResponse.BodyHandlers.ofString());

        List<StateQueryResponse> allAvroRecordsMetadata = objectMapper
            .readValue(allAvroRecordsMetadataResponse.body(), new TypeReference<>() {});

        assertEquals(200, allAvroRecordsMetadataResponse.statusCode());
        assertEquals("John", ((HashMap<?, ?>) allAvroRecordsMetadata.get(0).getKey()).get("firstName"));
        assertEquals("Doe", ((HashMap<?, ?>) allAvroRecordsMetadata.get(0).getKey()).get("lastName"));
        assertEquals("John", ((HashMap<?, ?>) allAvroRecordsMetadata.get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((HashMap<?, ?>) allAvroRecordsMetadata.get(0).getValue()).get("lastName"));
        assertNull(allAvroRecordsMetadata.get(0).getTimestamp());
        assertEquals("localhost", allAvroRecordsMetadata.get(0).getHostInfo().host());
        assertEquals(8081, allAvroRecordsMetadata.get(0).getHostInfo().port());
        assertEquals("appInteractiveQueriesId-REPARTITIONED-repartition",
            allAvroRecordsMetadata.get(0).getPositionVectors().get(0).topic());
        assertEquals(1, allAvroRecordsMetadata.get(0).getPositionVectors().get(0).partition());
        assertNotNull(allAvroRecordsMetadata.get(0).getPositionVectors().get(0).offset());
    }

    @Test
    void shouldGetAllInAvroAvroKeyValueStoreFromInteractiveQueriesService() {
        KafkaPersonStub key = KafkaPersonStub.newBuilder()
            .setId(1L)
            .setFirstName("John")
            .setLastName("Doe")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
            .build();

        List<StateQueryData<KafkaPersonStub, KafkaPersonStub>> stateQueryData = interactiveQueriesService
            .getAll("AVRO_AVRO_STORE", KafkaPersonStub.class, KafkaPersonStub.class);

        assertEquals(key, stateQueryData.get(0).getKey());
        assertEquals(key, stateQueryData.get(0).getValue());
        assertNull(stateQueryData.get(0).getTimestamp());
        assertEquals("localhost", stateQueryData.get(0).getHostInfo().host());
        assertEquals(8081, stateQueryData.get(0).getHostInfo().port());
        assertEquals("appInteractiveQueriesId-REPARTITIONED-repartition",
            stateQueryData.get(0).getPositionVectors().get(0).topic());
        assertEquals(1, stateQueryData.get(0).getPositionVectors().get(0).partition());
        assertNotNull(stateQueryData.get(0).getPositionVectors().get(0).offset());
    }

    @Test
    void shouldGetAllInStringAvroTimestampedKeyValueStore() throws IOException, InterruptedException {
        HttpRequest allAvroRecordsMetadataRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8081/store/key-value/STRING_AVRO_TIMESTAMPED_STORE?includeKey=true&includeMetadata=true"))
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
        assertEquals("INPUT_TOPIC", allAvroRecordsMetadata.get(0).getPositionVectors().get(0).topic());
        assertEquals(0, allAvroRecordsMetadata.get(0).getPositionVectors().get(0).partition());
        assertNotNull(allAvroRecordsMetadata.get(0).getPositionVectors().get(0).offset());
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
            KStream<String, KafkaPersonStub> stream = streamsBuilder
                .stream("INPUT_TOPIC", Consumed.with(Serdes.String(), SerdesUtils.<KafkaPersonStub>getValueSerdes()));

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

            // Change key and repartition so the record ends up in the partition 1 of the store
            stream
                .selectKey((key, value) -> value)
                .repartition(Repartitioned
                    .<KafkaPersonStub, KafkaPersonStub>with(SerdesUtils.getKeySerdes(), SerdesUtils.getValueSerdes())
                    .withName("REPARTITIONED"))
                .process(new ProcessorSupplier<KafkaPersonStub, KafkaPersonStub, KafkaPersonStub, KafkaPersonStub>() {
                    @Override
                    public Set<StoreBuilder<?>> stores() {
                        // <Avro, Avro> key-value store
                        StoreBuilder<KeyValueStore<KafkaPersonStub, KafkaPersonStub>> avroAvroKeyValueStoreBuilder =
                            Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore("AVRO_AVRO_STORE"),
                                SerdesUtils.getKeySerdes(), SerdesUtils.getValueSerdes());

                        return Set.of(
                            avroAvroKeyValueStoreBuilder
                        );
                    }

                    @Override
                    public Processor<KafkaPersonStub, KafkaPersonStub, KafkaPersonStub, KafkaPersonStub> get() {
                        return new Processor<>() {
                            private KeyValueStore<KafkaPersonStub, KafkaPersonStub> avroAvroKeyValueStore;

                            @Override
                            public void init(ProcessorContext<KafkaPersonStub, KafkaPersonStub> context) {
                                this.avroAvroKeyValueStore = context.getStateStore("AVRO_AVRO_STORE");
                            }

                            @Override
                            public void process(Record<KafkaPersonStub, KafkaPersonStub> message) {
                                avroAvroKeyValueStore.put(message.key(), message.value());
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
