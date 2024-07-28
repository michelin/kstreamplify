package com.michelin.kstreamplify.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.kstreamplify.exception.OtherInstanceResponseException;
import com.michelin.kstreamplify.exception.UnknownKeyException;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.store.HostInfoResponse;
import com.michelin.kstreamplify.store.StateQueryData;
import com.michelin.kstreamplify.store.StateQueryResponse;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Interactive queries service.
 */
@Slf4j
@AllArgsConstructor
public class InteractiveQueriesService {
    private static final String STREAMS_NOT_STARTED = "Cannot process request while instance is in %s state";
    private static final String UNKNOWN_STATE_STORE = "State store %s not found";
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient;

    @Getter
    private final KafkaStreamsInitializer kafkaStreamsInitializer;

    /**
     * The default store endpoint path.
     */
    public static final String DEFAULT_STORE_PATH = "store";

    /**
     * Constructor.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     */
    public InteractiveQueriesService(KafkaStreamsInitializer kafkaStreamsInitializer) {
        this.kafkaStreamsInitializer = kafkaStreamsInitializer;
        this.httpClient = HttpClient.newHttpClient();
    }

    /**
     * Get the stores.
     *
     * @return The stores
     */
    public List<String> getStores() {
        checkStreamsRunning();

        final Collection<StreamsMetadata> metadata = kafkaStreamsInitializer
            .getKafkaStreams()
            .metadataForAllStreamsClients();

        if (metadata == null || metadata.isEmpty()) {
            return Collections.emptyList();
        }

        return metadata
            .stream()
            .flatMap(streamsMetadata -> streamsMetadata.stateStoreNames().stream())
            .toList();
    }

    /**
     * Get the hosts of the store.
     *
     * @param store The store
     * @return The hosts
     */
    public Collection<StreamsMetadata> getStreamsMetadata(final String store) {
        checkStreamsRunning();

        return kafkaStreamsInitializer
            .getKafkaStreams()
            .streamsMetadataForStore(store);
    }

    /**
     * Get all values from the store.
     *
     * @param store The store
     * @param keyClass The key class
     * @param valueClass The value class
     * @param <K> The key type
     * @param <V> The value type
     * @return The values
     */
    public <K, V> List<StateQueryData<K, V>> getAll(String store, Class<K> keyClass, Class<V> valueClass) {
        final Collection<StreamsMetadata> streamsMetadata = getStreamsMetadata(store);

        if (streamsMetadata == null || streamsMetadata.isEmpty()) {
            throw new UnknownStateStoreException(String.format(UNKNOWN_STATE_STORE, store));
        }

        List<StateQueryData<K, V>> values = new ArrayList<>();
        streamsMetadata.forEach(metadata -> {
            if (isNotCurrentHost(metadata.hostInfo())) {
                log.debug("Fetching data on other instance ({}:{})", metadata.host(), metadata.port());

                List<StateQueryData<K, V>> stateQueryDataResponse = requestAllOtherInstance(metadata.hostInfo(),
                    "/store/key-value/" + store + "?includeKey=true&includeMetadata=true", keyClass, valueClass);
                values.addAll(stateQueryDataResponse);
            } else {
                log.debug("Fetching data on this instance ({}:{})", metadata.host(), metadata.port());

                RangeQuery<K, Object> rangeQuery = RangeQuery.withNoBounds();
                StateQueryResult<KeyValueIterator<K, Object>> result = kafkaStreamsInitializer
                    .getKafkaStreams()
                    .query(StateQueryRequest
                        .inStore(store)
                        .withQuery(rangeQuery)
                        .withPartitions(metadata.topicPartitions()
                            .stream()
                            .map(TopicPartition::partition)
                            .collect(Collectors.toSet())));

                List<StateQueryData<K, V>> partitionsResult = new ArrayList<>();
                result.getPartitionResults().forEach((key, queryResult) ->
                    queryResult.getResult().forEachRemaining(kv -> {
                        Set<String> topics = queryResult.getPosition().getTopics();
                        List<StateQueryResponse.PositionVector> positions = topics
                            .stream()
                            .flatMap(topic -> queryResult.getPosition().getPartitionPositions(topic)
                                .entrySet()
                                .stream()
                                .map(partitionOffset -> new StateQueryResponse.PositionVector(topic,
                                    partitionOffset.getKey(), partitionOffset.getValue())))
                            .toList();

                        V value;
                        Long timestamp;
                        if (kv.value instanceof ValueAndTimestamp<?>) {
                            ValueAndTimestamp<V> valueAndTimestamp = (ValueAndTimestamp<V>) kv.value;
                            value = valueAndTimestamp.value();
                            timestamp = valueAndTimestamp.timestamp();
                        } else {
                            value = (V) kv.value;
                            timestamp = null;
                        }

                        partitionsResult.add(new StateQueryData<>(kv.key, value, timestamp,
                            new HostInfoResponse(metadata.hostInfo().host(), metadata.hostInfo().port()),
                            positions));
                    }));

                values.addAll(partitionsResult);
            }
        });

        return values;
    }

    /**
     * Get the value by key from the store.
     *
     * @param store The store name
     * @param key   The key
     * @param serializer The serializer
     * @param valueClass The value class
     * @param <K> The key type
     * @param <V> The value type
     * @return The value
     */
    @SuppressWarnings("unchecked")
    public <K, V> StateQueryData<K, V> getByKey(String store, K key, Serializer<K> serializer, Class<V> valueClass) {
        KeyQueryMetadata keyQueryMetadata = getKeyQueryMetadata(store, key, serializer);

        if (keyQueryMetadata == null) {
            throw new UnknownStateStoreException(String.format(UNKNOWN_STATE_STORE, store));
        }

        HostInfo host = keyQueryMetadata.activeHost();
        if (isNotCurrentHost(host)) {
            log.debug("The key {} has been located on another instance ({}:{})", key,
                host.host(), host.port());

            Class<K> keyClass = (Class<K>) key.getClass();

            return requestOtherInstance(host, "/store/key-value/" + store + "/" + key
                + "?includeKey=true&includeMetadata=true", keyClass, valueClass);
        }

        log.debug("The key {} has been located on the current instance ({}:{})", key,
            host.host(), host.port());

        KeyQuery<K, Object> keyQuery = KeyQuery.withKey(key);
        StateQueryResult<Object> result = kafkaStreamsInitializer
            .getKafkaStreams()
            .query(StateQueryRequest
                .inStore(store)
                .withQuery(keyQuery)
                .withPartitions(Collections.singleton(keyQueryMetadata.partition())));

        if (result.getOnlyPartitionResult() == null) {
            throw new UnknownKeyException(key.toString());
        }

        Set<String> topics = result.getOnlyPartitionResult().getPosition().getTopics();
        List<StateQueryResponse.PositionVector> positions = topics
            .stream()
            .flatMap(topic -> result.getOnlyPartitionResult().getPosition().getPartitionPositions(topic)
                .entrySet()
                .stream()
                .map(partitionOffset -> new StateQueryResponse.PositionVector(topic,
                    partitionOffset.getKey(), partitionOffset.getValue())))
            .toList();

        V value;
        Long timestamp;
        if (result.getOnlyPartitionResult().getResult() instanceof ValueAndTimestamp<?>) {
            ValueAndTimestamp<V> valueAndTimestamp = (ValueAndTimestamp<V>) result.getOnlyPartitionResult().getResult();
            value = valueAndTimestamp.value();
            timestamp = valueAndTimestamp.timestamp();
        } else {
            value = (V) result.getOnlyPartitionResult().getResult();
            timestamp = null;
        }

        return new StateQueryData<>(key, value, timestamp,
            new HostInfoResponse(host.host(), host.port()),
            positions);
    }

    /**
     * Get the host by store and key.
     *
     * @param store The store
     * @param key   The key
     * @return The host
     */
    private <K> KeyQueryMetadata getKeyQueryMetadata(String store, K key, Serializer<K> serializer) {
        checkStreamsRunning();

        return kafkaStreamsInitializer
            .getKafkaStreams()
            .queryMetadataForKey(store, key, serializer);
    }

    /**
     * Request other instance.
     *
     * @param host        The host instance
     * @param endpointPath The endpoint path to request
     * @param keyClass The key class
     * @param valueClass The value class
     * @return The response
     */
    private <K, V> List<StateQueryData<K, V>> requestAllOtherInstance(HostInfo host,
                                                                      String endpointPath,
                                                                      Class<K> keyClass,
                                                                      Class<V> valueClass) {
        try {
            String jsonResponse = sendRequest(host, endpointPath);
            List<StateQueryResponse> response = objectMapper.readValue(jsonResponse, new TypeReference<>() {});
            return response
                .stream()
                .map(stateQueryResponse -> {
                    K key = objectMapper.convertValue(stateQueryResponse.getKey(), keyClass);
                    V value = objectMapper.convertValue(stateQueryResponse.getValue(), valueClass);

                    return new StateQueryData<>(key, value,
                        stateQueryResponse.getTimestamp(),
                        stateQueryResponse.getHostInfo(),
                        stateQueryResponse.getPositionVectors());
                })
                .toList();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Collections.emptyList();
        } catch (Exception e) {
            throw new OtherInstanceResponseException(e);
        }
    }

    /**
     * Request other instance.
     *
     * @param host        The host instance
     * @param endpointPath The endpoint path to request
     * @param keyClass The key class
     * @param valueClass The value class
     * @return The response
     */
    private <K, V> StateQueryData<K, V> requestOtherInstance(HostInfo host, String endpointPath, Class<K> keyClass,
                                                             Class<V> valueClass) {
        try {
            String jsonResponse = sendRequest(host, endpointPath);
            StateQueryResponse response = objectMapper.readValue(jsonResponse, StateQueryResponse.class);
            K key = objectMapper.convertValue(response.getKey(), keyClass);
            V value = objectMapper.convertValue(response.getValue(), valueClass);

            return new StateQueryData<>(key, value, response.getTimestamp(),
                response.getHostInfo(), response.getPositionVectors());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (Exception e) {
            throw new OtherInstanceResponseException(e);
        }
    }

    private String sendRequest(HostInfo host, String endpointPath)
        throws URISyntaxException, ExecutionException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
            .header("Accept", "application/json")
            .uri(new URI(String.format("http://%s:%d/%s", host.host(), host.port(), endpointPath)))
            .GET()
            .build();

        return httpClient
            .sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApply(HttpResponse::body)
            .get();
    }

    /**
     * Check if given host is equals to the current stream host.
     *
     * @param compareHostInfo The host to compare
     * @return True if the host is not the current host
     */
    private boolean isNotCurrentHost(HostInfo compareHostInfo) {
        return !kafkaStreamsInitializer.getHostInfo().host().equals(compareHostInfo.host())
            || kafkaStreamsInitializer.getHostInfo().port() != compareHostInfo.port();
    }

    /**
     * Check if the streams are started.
     */
    private void checkStreamsRunning() {
        if (kafkaStreamsInitializer.isNotRunning()) {
            KafkaStreams.State state = kafkaStreamsInitializer.getKafkaStreams().state();
            throw new StreamsNotStartedException(String.format(STREAMS_NOT_STARTED, state));
        }
    }
}
