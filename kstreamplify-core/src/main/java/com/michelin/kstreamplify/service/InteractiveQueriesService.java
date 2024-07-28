package com.michelin.kstreamplify.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.kstreamplify.exception.OtherInstanceResponseException;
import com.michelin.kstreamplify.exception.UnknownKeyException;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.store.StateStoreRecord;
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
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
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
    public Set<String> getStateStores() {
        checkStreamsRunning();

        final Collection<org.apache.kafka.streams.StreamsMetadata> metadata = kafkaStreamsInitializer
            .getKafkaStreams()
            .metadataForAllStreamsClients();

        if (metadata == null || metadata.isEmpty()) {
            return Collections.emptySet();
        }

        return metadata
            .stream()
            .flatMap(streamsMetadata -> streamsMetadata.stateStoreNames().stream())
            .collect(Collectors.toSet());
    }

    /**
     * Get the hosts of the store.
     *
     * @param store The store
     * @return The hosts
     */
    public Collection<org.apache.kafka.streams.StreamsMetadata> getStreamsMetadataForStore(final String store) {
        checkStreamsRunning();

        return kafkaStreamsInitializer
            .getKafkaStreams()
            .streamsMetadataForStore(store);
    }

    /**
     * Get all values from the store.
     *
     * @param store The store
     * @return The values
     */
    @SuppressWarnings("unchecked")
    public List<StateStoreRecord> getAll(String store) {
        final Collection<StreamsMetadata> streamsMetadata = getStreamsMetadataForStore(store);

        if (streamsMetadata == null || streamsMetadata.isEmpty()) {
            throw new UnknownStateStoreException(String.format(UNKNOWN_STATE_STORE, store));
        }

        List<StateStoreRecord> results = new ArrayList<>();
        streamsMetadata.forEach(metadata -> {
            if (isNotCurrentHost(metadata.hostInfo())) {
                log.debug("Fetching data on other instance ({}:{})", metadata.host(), metadata.port());

                List<StateStoreRecord> stateStoreRecordResponse = requestAllOtherInstance(metadata.hostInfo(),
                    "/store/key-value/" + store);
                results.addAll(stateStoreRecordResponse);
            } else {
                log.debug("Fetching data on this instance ({}:{})", metadata.host(), metadata.port());

                RangeQuery<String, Object> rangeQuery = RangeQuery.withNoBounds();
                StateQueryResult<KeyValueIterator<String, Object>> result = kafkaStreamsInitializer
                    .getKafkaStreams()
                    .query(StateQueryRequest
                        .inStore(store)
                        .withQuery(rangeQuery)
                        .withPartitions(metadata.topicPartitions()
                            .stream()
                            .map(TopicPartition::partition)
                            .collect(Collectors.toSet())));

                List<StateStoreRecord> partitionsResult = new ArrayList<>();
                result.getPartitionResults().forEach((key, queryResult) ->
                    queryResult.getResult().forEachRemaining(kv -> {
                        if (kv.value instanceof ValueAndTimestamp<?>) {
                            ValueAndTimestamp<Object> valueAndTimestamp = (ValueAndTimestamp<Object>) kv.value;

                            partitionsResult.add(
                                new StateStoreRecord(kv.key, valueAndTimestamp.value(), valueAndTimestamp.timestamp())
                            );
                        } else {
                            partitionsResult.add(new StateStoreRecord(kv.key, kv.value));
                        }
                    }));

                results.addAll(partitionsResult);
            }
        });

        return results;
    }

    /**
     * Get the value by key from the store.
     *
     * @param store The store name
     * @param key   The key
     * @return The value
     */
    @SuppressWarnings("unchecked")
    public StateStoreRecord getByKey(String store, String key) {
        KeyQueryMetadata keyQueryMetadata = getKeyQueryMetadata(store, key, new StringSerializer());

        if (keyQueryMetadata == null) {
            throw new UnknownStateStoreException(String.format(UNKNOWN_STATE_STORE, store));
        }

        HostInfo host = keyQueryMetadata.activeHost();
        if (isNotCurrentHost(host)) {
            log.debug("The key {} has been located on another instance ({}:{})", key,
                host.host(), host.port());

            return requestOtherInstance(host, "/store/key-value/" + store + "/" + key);
        }

        log.debug("The key {} has been located on the current instance ({}:{})", key,
            host.host(), host.port());

        KeyQuery<String, Object> keyQuery = KeyQuery.withKey(key);
        StateQueryResult<Object> result = kafkaStreamsInitializer
            .getKafkaStreams()
            .query(StateQueryRequest
                .inStore(store)
                .withQuery(keyQuery)
                .withPartitions(Collections.singleton(keyQueryMetadata.partition())));

        if (result.getOnlyPartitionResult() == null) {
            throw new UnknownKeyException(key);
        }

        if (result.getOnlyPartitionResult().getResult() instanceof ValueAndTimestamp<?>) {
            ValueAndTimestamp<Object> valueAndTimestamp = (ValueAndTimestamp<Object>) result.getOnlyPartitionResult()
                .getResult();

            return new StateStoreRecord(key, valueAndTimestamp.value(), valueAndTimestamp.timestamp());
        }

        return new StateStoreRecord(key, result.getOnlyPartitionResult().getResult());
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
     * @return The response
     */
    private List<StateStoreRecord> requestAllOtherInstance(HostInfo host, String endpointPath) {
        try {
            String jsonResponse = sendRequest(host, endpointPath);
            return objectMapper.readValue(jsonResponse, new TypeReference<>() {});
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
     * @return The response
     */
    private StateStoreRecord requestOtherInstance(HostInfo host, String endpointPath) {
        try {
            String jsonResponse = sendRequest(host, endpointPath);
            return objectMapper.readValue(jsonResponse, StateStoreRecord.class);
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
