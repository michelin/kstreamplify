package com.michelin.kstreamplify.controller;

import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.model.HostInfoResponse;
import com.michelin.kstreamplify.model.QueryResponse;
import com.michelin.kstreamplify.service.InteractiveQueriesService;
import java.util.List;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
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
        if (interactiveQueriesService.getKafkaStreamsInitializer().isNotRunning()) {
            KafkaStreams.State state = interactiveQueriesService.getKafkaStreamsInitializer().getKafkaStreams().state();
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
        if (interactiveQueriesService.getKafkaStreamsInitializer().isNotRunning()) {
            KafkaStreams.State state = interactiveQueriesService.getKafkaStreamsInitializer().getKafkaStreams().state();
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
     * @param includeKey Include the key in the response
     * @param includeMetadata Include the metadata in the response
     * @return The values
     */
    @GetMapping(value = "/{store}")
    public ResponseEntity<List<QueryResponse>> getAll(@PathVariable("store") String store,
                                                      @RequestParam(value = "includeKey", required = false)
                                                      Boolean includeKey,
                                                      @RequestParam(value = "includeMetadata", required = false)
                                                      Boolean includeMetadata) {
        if (interactiveQueriesService.getKafkaStreamsInitializer().isNotRunning()) {
            KafkaStreams.State state = interactiveQueriesService.getKafkaStreamsInitializer().getKafkaStreams().state();
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
     * @param includeKey Include the key in the response
     * @param includeMetadata Include the metadata in the response
     * @return The value
     */
    @GetMapping("/{store}/{key}")
    public ResponseEntity<QueryResponse> getByKey(@PathVariable("store") String store,
                                                  @PathVariable("key") String key,
                                                  @RequestParam(value = "includeKey", required = false)
                                                  Boolean includeKey,
                                                  @RequestParam(value = "includeMetadata", required = false)
                                                  Boolean includeMetadata) {
        if (interactiveQueriesService.getKafkaStreamsInitializer().isNotRunning()) {
            KafkaStreams.State state = interactiveQueriesService.getKafkaStreamsInitializer().getKafkaStreams().state();
            throw new StreamsNotStartedException(String.format(STREAMS_NOT_STARTED, state));
        }

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(interactiveQueriesService.getByKey(store, key, new StringSerializer(), includeKey, includeMetadata));
    }
}
