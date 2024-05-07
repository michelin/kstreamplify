package com.michelin.kstreamplify.http.service;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/**
 * Interactive queries service.
 */
@Slf4j
@AllArgsConstructor
public class InteractiveQueriesService {
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final KafkaStreamsInitializer kafkaStreamsInitializer;

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
    public List<HostInfo> getHostsByStore(final String store) {
        final Collection<StreamsMetadata> metadata = kafkaStreamsInitializer
            .getKafkaStreams()
            .streamsMetadataForStore(store);

        if (metadata == null || metadata.isEmpty()) {
            return Collections.emptyList();
        }

        return metadata
            .stream()
            .map(StreamsMetadata::hostInfo)
            .toList();
    }

    /**
     * Get the value by key from the store.
     *
     * @param store The store name
     * @param key   The key
     * @return The value
     */
    public <K> Object getByKey(String store, K key, Serializer<K> serializer) {
        final HostInfo host = getHostByStoreAndKey(store, key, serializer);

        if (isNotCurrentHost(host)) {
            log.info("The key {} has been located on another instance ({}:{})", key,
                host.host(), host.port());

            return requestOtherInstance(host, "/store/" + store + "/" + key);
        }

        log.debug("The key {} has been located on the current instance ({}:{})", key,
            host.host(), host.port());

        final ReadOnlyKeyValueStore<K, Object> readOnlyStore = kafkaStreamsInitializer.getKafkaStreams().store(
            StoreQueryParameters.fromNameAndType(store, QueryableStoreTypes.keyValueStore()));

        Object value = readOnlyStore.get(key);
        if (value == null) {
            log.debug("No value found for the key {}.", key);
            return null;
        }

        return value;
    }

    /**
     * Get all values from the store.
     *
     * @param store The store
     * @return The values
     */
    public List<Object> getAll(String store) {
        final List<HostInfo> hosts = getHostsByStore(store);

        if (hosts.isEmpty()) {
            log.debug("No host found for the given state store {}", store);
            return null;
        }

        List<Object> values = new ArrayList<>();
        hosts.forEach(host -> {
            if (isNotCurrentHost(host)) {
                log.debug("Fetching data on other instance ({}:{})", host.host(), host.port());
                values.addAll(requestOtherInstance(host, "/store/" + store));
            } else {
                log.debug("Fetching data on this instance ({}:{})", host.host(), host.port());
                values.addAll(getAllOnCurrentInstance(store)
                    .stream()
                    .map(entry -> entry.value)
                    .toList());
            }
        });

        return values;
    }

    private <K> List<KeyValue<K, Object>> getAllOnCurrentInstance(String storeName) {
        final ReadOnlyKeyValueStore<K, Object> store = kafkaStreamsInitializer.getKafkaStreams().store(
            StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));

        List<KeyValue<K, Object>> results = new ArrayList<>();
        try (KeyValueIterator<K, Object> iterator = store.all()) {
            while (iterator.hasNext()) {
                results.add(iterator.next());
            }
        }

        return results;
    }

    /**
     * Get the host by store and key.
     *
     * @param store The store
     * @param key   The key
     * @return The host
     */
    private <K> HostInfo getHostByStoreAndKey(final String store, final K key, Serializer<K> serializer) {
        final KeyQueryMetadata metadata = kafkaStreamsInitializer
            .getKafkaStreams()
            .queryMetadataForKey(store, key, serializer);

        if (metadata == null) {
            return null;
        }

        return metadata.activeHost();
    }

    private String requestOtherInstance(HostInfo host, String endpointPath) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .header("Accept", "application/json")
                .uri(new URI(String.format("http://%s:%d/%s", host.host(), host.port(), endpointPath)))
                .GET()
                .build();

            return httpClient
                .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(HttpResponse::body)
                .get();
        } catch (URISyntaxException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isNotCurrentHost(HostInfo compareHostInfo) {
        return !kafkaStreamsInitializer.getHostInfo().host().equals(compareHostInfo.host())
            || kafkaStreamsInitializer.getHostInfo().port() != compareHostInfo.port();
    }
}
