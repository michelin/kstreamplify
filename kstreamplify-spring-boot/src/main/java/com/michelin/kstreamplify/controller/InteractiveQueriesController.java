package com.michelin.kstreamplify.controller;

import static com.michelin.kstreamplify.converter.AvroToJsonConverter.convertToJson;

import com.michelin.kstreamplify.http.service.HostInfoResponse;
import com.michelin.kstreamplify.http.service.InteractiveQueriesService;
import com.michelin.kstreamplify.http.service.QueryResponse;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import java.util.List;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Kafka Streams controller for store.
 */
@RestController
@RequestMapping("/store")
@ConditionalOnBean(KafkaStreamsStarter.class)
public class InteractiveQueriesController {
    private static final String STREAMS_NOT_STARTED = "Cannot process request while instance is in %s state";

    /**
     * The store service.
     */
    @Autowired
    private InteractiveQueriesService interactiveQueriesService;

    /**
     * Get the stores.
     *
     * @return The stores
     */
    @GetMapping
    public ResponseEntity<List<String>> getStores() {
        KafkaStreams.State state = interactiveQueriesService.getKafkaStreamsInitializer().getKafkaStreams().state();
        if (!interactiveQueriesService.getKafkaStreamsInitializer().isRunning()) {
            throw new StreamsNotStartedException(String.format(STREAMS_NOT_STARTED, state));
        }

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(interactiveQueriesService.getStores());
    }

    /**
     * Get the hosts of the store.
     *
     * @param store The store
     * @return The hosts
     */
    @GetMapping(value = "/{store}/info")
    public ResponseEntity<List<HostInfoResponse>> getHostsForStore(@PathVariable("store") final String store) {
        KafkaStreams.State state = interactiveQueriesService.getKafkaStreamsInitializer().getKafkaStreams().state();
        if (!interactiveQueriesService.getKafkaStreamsInitializer().isRunning()) {
            throw new StreamsNotStartedException(String.format(STREAMS_NOT_STARTED, state));
        }

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(interactiveQueriesService.getStreamsMetadata(store)
                .stream()
                .map(streamsMetadata -> new HostInfoResponse(streamsMetadata.host(), streamsMetadata.port()))
                .toList());
    }

    /**
     * Get all the values from the store.
     *
     * @param store The store
     * @return The values
     */
    @GetMapping(value = "/{store}")
    public ResponseEntity<List<QueryResponse>> getAll(@PathVariable("store") String store,
                                                      @RequestParam(value = "includeKey", required = false)
                                                      boolean includeKey,
                                                      @RequestParam(value = "includeMetadata", required = false)
                                                      boolean includeMetadata) {
        KafkaStreams.State state = interactiveQueriesService.getKafkaStreamsInitializer().getKafkaStreams().state();
        if (!interactiveQueriesService.getKafkaStreamsInitializer().isRunning()) {
            throw new StreamsNotStartedException(String.format(STREAMS_NOT_STARTED, state));
        }

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(interactiveQueriesService.getAll(store, includeKey, includeMetadata));
    }

    /**
     * Get the key-value by key from the store.
     *
     * @param store The store
     * @param key The key
     * @return The value
     */
    @GetMapping("/{store}/{key}")
    public ResponseEntity<QueryResponse> getByKey(@PathVariable("store") String store,
                                                  @PathVariable("key") String key,
                                                  @RequestParam(value = "includeKey", required = false)
                                                  boolean includeKey,
                                                  @RequestParam(value = "includeMetadata", required = false)
                                                  boolean includeMetadata) {
        KafkaStreams.State state = interactiveQueriesService.getKafkaStreamsInitializer().getKafkaStreams().state();
        if (!interactiveQueriesService.getKafkaStreamsInitializer().isRunning()) {
            throw new StreamsNotStartedException(String.format(STREAMS_NOT_STARTED, state));
        }

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(interactiveQueriesService.getByKey(store, key, new StringSerializer(), includeKey, includeMetadata));
    }
}
