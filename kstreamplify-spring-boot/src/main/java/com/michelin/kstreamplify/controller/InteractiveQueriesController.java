package com.michelin.kstreamplify.controller;

import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.service.InteractiveQueriesService;
import com.michelin.kstreamplify.store.StateStoreRecord;
import com.michelin.kstreamplify.store.StreamsMetadata;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@Tag(name = "Interactive Queries", description = "Interactive Queries Controller")
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
    @Operation(summary = "Get the state stores")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "OK", content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = Set.class))
        }),
    })
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
    @Operation(summary = "Get the streams metadata for a store")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "OK", content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = StreamsMetadata.class))
        }),
        @ApiResponse(responseCode = "503", description = "Kafka Streams not running", content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = String.class))
        }),
    })
    @GetMapping(value = "/metadata/{store}")
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
     * Get all records from the store.
     *
     * @param store The store
     * @return The values
     */
    @Operation(summary = "Get all records from a store")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "OK", content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = List.class))
        }),
        @ApiResponse(responseCode = "404", description = "Store not found", content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = String.class))
        }),
        @ApiResponse(responseCode = "503", description = "Kafka Streams not running", content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = String.class))
        }),
    })
    @GetMapping(value = "/{store}")
    public ResponseEntity<List<StateStoreRecord>> getAll(@PathVariable("store") String store) {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(interactiveQueriesService.getAll(store));
    }

    /**
     * Get all records from the store on the local host.
     *
     * @param store The store
     * @return The values
     */
    @Operation(summary = "Get all records from a store on the local host")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "OK", content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = List.class))
        }),
        @ApiResponse(responseCode = "404", description = "Store not found", content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = String.class))
        }),
        @ApiResponse(responseCode = "503", description = "Kafka Streams not running", content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = String.class))
        }),
    })
    @GetMapping(value = "/local/{store}")
    public ResponseEntity<List<StateStoreRecord>> getAllOnLocalhost(@PathVariable("store") String store) {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(interactiveQueriesService.getAllOnLocalhost(store));
    }

    /**
     * Get the record by key from the store.
     *
     * @param store The store
     * @param key The key
     * @return The value
     */
    @Operation(summary = "Get a record by key from a store")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "OK", content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = StateStoreRecord.class))
        }),
        @ApiResponse(responseCode = "404", description = "Key not found", content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = String.class))
        }),
        @ApiResponse(responseCode = "404", description = "Store not found", content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = String.class))
        }),
        @ApiResponse(responseCode = "503", description = "Kafka Streams not running", content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = String.class))
        }),
    })
    @GetMapping("/{store}/{key}")
    public ResponseEntity<StateStoreRecord> getByKey(@PathVariable("store") String store,
                                                     @PathVariable("key") String key) {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(interactiveQueriesService.getByKey(store, key));
    }
}
