package com.michelin.kstreamplify.service.interactivequeries;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.kstreamplify.exception.OtherInstanceResponseException;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.store.StateStoreRecord;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.state.HostInfo;

/**
 * Interactive queries service.
 */
@Slf4j
@AllArgsConstructor
abstract class InteractiveQueriesService {
    private static final String STREAMS_NOT_STARTED = "Cannot process request while instance is in %s state";
    protected static final String UNKNOWN_STATE_STORE = "State store %s not found";
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient;

    @Getter
    protected final KafkaStreamsInitializer kafkaStreamsInitializer;

    /**
     * Constructor.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     */
    protected InteractiveQueriesService(KafkaStreamsInitializer kafkaStreamsInitializer) {
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
    public Collection<StreamsMetadata> getStreamsMetadataForStore(final String store) {
        checkStreamsRunning();

        return kafkaStreamsInitializer
            .getKafkaStreams()
            .streamsMetadataForStore(store);
    }

    /**
     * Get the host by store and key.
     *
     * @param store The store
     * @param key   The key
     * @return The host
     */
    protected <K> KeyQueryMetadata getKeyQueryMetadata(String store, K key, Serializer<K> serializer) {
        checkStreamsRunning();

        return kafkaStreamsInitializer
            .getKafkaStreams()
            .queryMetadataForKey(store, key, serializer);
    }

    /**
     * Request remote instance.
     *
     * @param host        The host instance
     * @param endpointPath The endpoint path to request
     * @return The response
     */
    protected List<StateStoreRecord> getAllOnRemoteHost(HostInfo host, String endpointPath) {
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
     * Request remote instance.
     *
     * @param host        The host instance
     * @param endpointPath The endpoint path to request
     * @return The response
     */
    protected StateStoreRecord getByKeyOnRemoteHost(HostInfo host, String endpointPath) {
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

    /**
     * Send request to the remote host.
     *
     * @param host          The host
     * @param endpointPath The endpoint path
     * @return The response
     * @throws URISyntaxException URI syntax exception
     * @throws ExecutionException Execution exception
     * @throws InterruptedException Interrupted exception
     */
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
    protected boolean isNotCurrentHost(HostInfo compareHostInfo) {
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
