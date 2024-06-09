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
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Interactive queries service.
 */
@Slf4j
@AllArgsConstructor
public class InteractiveQueriesService {
    private static final String UNKNOWN_STATE_STORE = "State store %s not found";
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient;

    @Getter
    private final KafkaStreamsInitializer kafkaStreamsInitializer;

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
        return kafkaStreamsInitializer
            .getKafkaStreams()
            .streamsMetadataForStore(store);
    }

    /**
     * Get all values from the store.
     *
     * @param store The store
     * @param includeKey Include the key
     * @param includeMetadata Include the metadata
     * @return The values
     */
    public <K, V> List<StateQueryData<K, V>> getAll(String store, Class<K> keyClass, Class<V> valueClass,
                                                    boolean includeKey, boolean includeMetadata) {
        final Collection<StreamsMetadata> streamsMetadata = getStreamsMetadata(store);

        if (streamsMetadata == null || streamsMetadata.isEmpty()) {
            throw new UnknownStateStoreException(String.format(UNKNOWN_STATE_STORE, store));
        }

        List<StateQueryData<K, V>> values = new ArrayList<>();
        streamsMetadata.forEach(metadata -> {
            if (isNotCurrentHost(metadata.hostInfo())) {
                log.debug("Fetching data on other instance ({}:{})", metadata.host(), metadata.port());

                List<StateQueryData<K, V>> stateQueryDataResponse = requestAllOtherInstance(metadata.hostInfo(),
                    "/store/" + store + "?includeKey=" + includeKey + "&includeMetadata=" + includeMetadata,
                    keyClass, valueClass);
                values.addAll(stateQueryDataResponse);
            } else {
                log.debug("Fetching data on this instance ({}:{})", metadata.host(), metadata.port());

                List<StateQueryData<K, V>> partitionsResult = executeRangeQuery(store, metadata,
                    includeKey, includeMetadata);
                values.addAll(partitionsResult);
            }
        });

        return values;
    }

    private <K, V> List<StateQueryData<K, V>> executeRangeQuery(String store, StreamsMetadata metadata,
                                                                boolean includeKey, boolean includeMetadata) {
        RangeQuery<K, ValueAndTimestamp<V>> rangeQuery = RangeQuery.withNoBounds();
        StateQueryResult<KeyValueIterator<K, ValueAndTimestamp<V>>> result = kafkaStreamsInitializer
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
                StateQueryData<K, V> stateQueryData;
                if (includeMetadata) {
                    Set<String> topics = queryResult.getPosition().getTopics();
                    List<StateQueryResponse.PositionVector> positions = topics
                        .stream()
                        .flatMap(topic -> queryResult.getPosition().getPartitionPositions(topic)
                            .entrySet()
                            .stream()
                            .map(partitionOffset -> new StateQueryResponse.PositionVector(topic,
                                partitionOffset.getKey(), partitionOffset.getValue())))
                        .toList();

                    stateQueryData = new StateQueryData<>(includeKey ? kv.key : null,
                        kv.value.value(),
                        kv.value.timestamp(),
                        new HostInfoResponse(metadata.hostInfo().host(), metadata.hostInfo().port()),
                        positions);
                } else {
                    stateQueryData = new StateQueryData<>(includeKey ? kv.key : null, kv.value.value());
                }

                partitionsResult.add(stateQueryData);
            }));

        return partitionsResult;
    }

    /**
     * Get the value by key from the store.
     *
     * @param store The store name
     * @param key   The key
     * @param serializer The serializer
     * @param includeKey Include the key
     * @param includeMetadata Include the metadata
     * @param <K> The key type
     * @return The value
     */
    public <K, V> StateQueryData<K, V> getByKey(String store, K key, Serializer<K> serializer, Class<V> valueClass,
                                                boolean includeKey, boolean includeMetadata) {
        KeyQueryMetadata keyQueryMetadata = getKeyQueryMetadata(store, key, serializer);

        if (keyQueryMetadata == null) {
            throw new UnknownStateStoreException(String.format(UNKNOWN_STATE_STORE, store));
        }

        HostInfo host = keyQueryMetadata.activeHost();
        if (isNotCurrentHost(host)) {
            log.debug("The key {} has been located on another instance ({}:{})", key,
                host.host(), host.port());

            Class<K> keyClass = (Class<K>) key.getClass();

            return requestOtherInstance(host, "/store/" + store + "/" + key
                + "?includeKey=" + includeKey + "&includeMetadata=" + includeMetadata, keyClass, valueClass);
        }

        log.debug("The key {} has been located on the current instance ({}:{})", key,
            host.host(), host.port());

        KeyQuery<K, ValueAndTimestamp<V>> keyQuery = KeyQuery.withKey(key);
        StateQueryResult<ValueAndTimestamp<V>> result = kafkaStreamsInitializer
            .getKafkaStreams()
            .query(StateQueryRequest
                .inStore(store)
                .withQuery(keyQuery)
                .withPartitions(Collections.singleton(keyQueryMetadata.partition())));

        if (result.getOnlyPartitionResult() == null) {
            throw new UnknownKeyException(key.toString());
        }

        if (includeMetadata) {
            Set<String> topics = result.getOnlyPartitionResult().getPosition().getTopics();
            List<StateQueryResponse.PositionVector> positions = topics
                .stream()
                .flatMap(topic -> result.getOnlyPartitionResult().getPosition().getPartitionPositions(topic)
                    .entrySet()
                    .stream()
                    .map(partitionOffset -> new StateQueryResponse.PositionVector(topic,
                        partitionOffset.getKey(), partitionOffset.getValue())))
                .toList();

            return new StateQueryData<>(includeKey ? key : null, result.getOnlyPartitionResult().getResult().value(),
                result.getOnlyPartitionResult().getResult().timestamp(),
                new HostInfoResponse(host.host(), host.port()),
                positions);
        }

        return new StateQueryData<>(includeKey ? key : null, result.getOnlyPartitionResult().getResult().value());
    }

    /**
     * Get the host by store and key.
     *
     * @param store The store
     * @param key   The key
     * @return The host
     */
    private <K> KeyQueryMetadata getKeyQueryMetadata(String store, K key, Serializer<K> serializer) {
        return kafkaStreamsInitializer
            .getKafkaStreams()
            .queryMetadataForKey(store, key, serializer);
    }

    /**
     * Request other instance.
     *
     * @param host        The host instance
     * @param endpointPath The endpoint path to request
     * @return The response
     */
    private <K, V> List<StateQueryData<K, V>> requestAllOtherInstance(HostInfo host, String endpointPath,
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
        } catch (Exception e) {
            throw new OtherInstanceResponseException(e);
        }
    }

    /**
     * Request other instance.
     *
     * @param host        The host instance
     * @param endpointPath The endpoint path to request
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

    private boolean isNotCurrentHost(HostInfo compareHostInfo) {
        return !kafkaStreamsInitializer.getHostInfo().host().equals(compareHostInfo.host())
            || kafkaStreamsInitializer.getHostInfo().port() != compareHostInfo.port();
    }
}
