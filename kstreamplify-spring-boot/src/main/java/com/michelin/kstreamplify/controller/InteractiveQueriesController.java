package com.michelin.kstreamplify.controller;

import static com.michelin.kstreamplify.converter.AvroToJsonConverter.convertToJson;

import com.michelin.kstreamplify.http.exception.InstanceNotReadyException;
import com.michelin.kstreamplify.http.service.InteractiveQueriesService;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import java.util.List;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
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
    public ResponseEntity<List<String>> getStores() {
        if (!interactiveQueriesService.getKafkaStreamsInitializer().isRunning()) {
            throw new InstanceNotReadyException(interactiveQueriesService.getKafkaStreamsInitializer()
                .getKafkaStreams().state());
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
    @GetMapping(value = "/info/{store}")
    public ResponseEntity<List<String>> getHostsInfoForStore(@PathVariable("store") final String store) {
        if (!interactiveQueriesService.getKafkaStreamsInitializer().isRunning()) {
            throw new InstanceNotReadyException(interactiveQueriesService.getKafkaStreamsInitializer()
                .getKafkaStreams().state());
        }

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(interactiveQueriesService.getHostsByStore(store)
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
        if (!interactiveQueriesService.getKafkaStreamsInitializer().isRunning()) {
            throw new InstanceNotReadyException(interactiveQueriesService.getKafkaStreamsInitializer()
                .getKafkaStreams().state());
        }

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(convertToJson(interactiveQueriesService.getAll(store)));
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
        if (!interactiveQueriesService.getKafkaStreamsInitializer().isRunning()) {
            throw new InstanceNotReadyException(interactiveQueriesService.getKafkaStreamsInitializer()
                .getKafkaStreams().state());
        }

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(convertToJson(interactiveQueriesService.getByKey(store, key, new StringSerializer())));
    }
}
