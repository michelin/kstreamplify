package com.michelin.kstreamplify.controller;

import static com.michelin.kstreamplify.converter.AvroToJsonConverter.convertToJson;

import com.michelin.kstreamplify.http.service.StoreService;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import java.util.List;
import org.apache.kafka.common.serialization.StringSerializer;
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
public class StoreController {
    /**
     * The store service.
     */
    @Autowired
    private StoreService storeService;

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
            .body(storeService.getStores());
    }

    /**
     * Get the hosts of the store.
     *
     * @param store The store
     * @return The hosts
     */
    @GetMapping(value = "/info/{store}")
    public ResponseEntity<List<String>> getHostsInfoForStore(@PathVariable("store") final String store) {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(storeService.getHostsByStore(store)
                .stream()
                .map(host -> host.host() + ":" + host.port())
                .toList());
    }

    /**
     * Get all the values from the store.
     *
     * @param store The store
     * @return The values
     */
    @GetMapping(value = "/{store}")
    public ResponseEntity<String> getAll(@PathVariable("store") String store) {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(convertToJson(storeService.getAll(store)));
    }

    /**
     * Get the value by key from the store.
     *
     * @param key The key
     * @return The value
     */
    @GetMapping("/{store}/{key}")
    public ResponseEntity<String> getByKey(@PathVariable("store") String store,
                                           @PathVariable("key") String key) {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(convertToJson(storeService.getByKey(store, key, new StringSerializer())));
    }
}
