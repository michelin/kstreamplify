package com.michelin.kstreamplify.controller;

import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.service.InteractiveQueriesService;
import com.michelin.kstreamplify.store.StateStoreRecord;
import com.michelin.kstreamplify.store.StreamsMetadata;
import java.util.List;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Kafka Streams controller for store.
 */
@RestController
@RequestMapping("/store")
@ConditionalOnBean(KafkaStreamsStarter.class)
public class InteractiveQueriesController {

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
    public ResponseEntity<Set<String>> getStores() {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(interactiveQueriesService.getStateStores());
    }

    /**
     * Get the hosts of the store.
     *
     * @param store The store
     * @return The hosts
     */
    @GetMapping(value = "/{store}/metadata")
    public ResponseEntity<List<StreamsMetadata>> getStreamsMetadataForStore(@PathVariable("store") final String store) {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(interactiveQueriesService.getStreamsMetadataForStore(store)
                .stream()
                .map(streamsMetadata -> new StreamsMetadata(
                    streamsMetadata.stateStoreNames(),
                    streamsMetadata.hostInfo(),
                    streamsMetadata.topicPartitions()))
                .toList()
            );
    }

    /**
     * Get all the values from the store.
     *
     * @param store The store
     * @return The values
     */
    @GetMapping(value = "/key-value/{store}")
    public ResponseEntity<List<StateStoreRecord>> getAll(@PathVariable("store") String store) {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(interactiveQueriesService.getAll(store));
    }

    /**
     * Get the key-value by key from the store.
     *
     * @param store The store
     * @param key The key
     * @return The value
     */
    @GetMapping("/key-value/{store}/{key}")
    public ResponseEntity<StateStoreRecord> getByKey(@PathVariable("store") String store,
                                                     @PathVariable("key") String key) {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(interactiveQueriesService.getByKey(store, key));
    }
}
