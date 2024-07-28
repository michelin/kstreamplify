package com.michelin.kstreamplify.controller;

import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.service.InteractiveQueriesService;
import com.michelin.kstreamplify.store.HostInfoResponse;
import com.michelin.kstreamplify.store.StateQueryData;
import com.michelin.kstreamplify.store.StateQueryResponse;
import java.util.List;
import org.apache.kafka.common.serialization.StringSerializer;
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
    @GetMapping(value = "/key-value/{store}")
    public ResponseEntity<List<StateQueryResponse>> getAll(@PathVariable("store") String store,
                                                           @RequestParam(value = "includeKey", required = false,
                                                               defaultValue = "false") Boolean includeKey,
                                                           @RequestParam(value = "includeMetadata", required = false,
                                                               defaultValue = "false") Boolean includeMetadata) {
        List<StateQueryData<Object, Object>> stateQueryData = interactiveQueriesService
            .getAll(store, Object.class, Object.class);

        List<StateQueryResponse> stateQueryResponse = stateQueryData
            .stream()
            .map(stateQueryDataItem -> stateQueryDataItem.toStateQueryResponse(includeKey, includeMetadata))
            .toList();

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(stateQueryResponse);
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
    @GetMapping("/key-value/{store}/{key}")
    public ResponseEntity<StateQueryResponse> getByKey(@PathVariable("store") String store,
                                                       @PathVariable("key") String key,
                                                       @RequestParam(value = "includeKey", required = false,
                                                       defaultValue = "false") Boolean includeKey,
                                                       @RequestParam(value = "includeMetadata", required = false,
                                                       defaultValue = "false") Boolean includeMetadata) {
        StateQueryData<String, Object> stateQueryData = interactiveQueriesService
            .getByKey(store, key, new StringSerializer(), Object.class);

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(stateQueryData.toStateQueryResponse(includeKey, includeMetadata));
    }
}
