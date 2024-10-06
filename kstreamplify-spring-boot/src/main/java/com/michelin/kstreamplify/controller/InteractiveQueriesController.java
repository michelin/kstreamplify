package com.michelin.kstreamplify.controller;

import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.service.interactivequeries.KeyValueStoreService;
import com.michelin.kstreamplify.service.interactivequeries.WindowStoreService;
import com.michelin.kstreamplify.store.StateStoreRecord;
import com.michelin.kstreamplify.store.StreamsMetadata;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
@Tag(name = "Interactive Queries", description = "Interactive Queries Controller")
public class InteractiveQueriesController {
    @Autowired
    private KeyValueStoreService keyValueStoreService;

    @Autowired
    private WindowStoreService windowStoreService;

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
            .body(keyValueStoreService.getStateStores());
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
            .body(keyValueStoreService.getStreamsMetadataForStore(store)
                .stream()
                .map(streamsMetadata -> new StreamsMetadata(
                    streamsMetadata.stateStoreNames(),
                    streamsMetadata.hostInfo(),
                    streamsMetadata.topicPartitions()))
                .toList()
            );
    }

    /**
     * Get all records from the key-value store.
     *
     * @param store The store
     * @return The values
     */
    @Operation(summary = "Get all records from a key-value store")
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
    @GetMapping(value = "/key-value/{store}")
    public ResponseEntity<List<StateStoreRecord>> getAllInKeyValueStore(@PathVariable("store") String store) {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(keyValueStoreService.getAll(store));
    }

    /**
     * Get all records from the key-value store on the local host.
     *
     * @param store The store
     * @return The values
     */
    @Operation(summary = "Get all records from a key-value store on the local host")
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
    @GetMapping(value = "/key-value/local/{store}")
    public ResponseEntity<List<StateStoreRecord>> getAllInKeyValueStoreOnLocalHost(@PathVariable("store")
                                                                                       String store) {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(keyValueStoreService.getAllOnLocalHost(store));
    }

    /**
     * Get the record by key from the key-value store.
     *
     * @param store The store
     * @param key The key
     * @return The value
     */
    @Operation(summary = "Get a record by key from a key-value store")
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
    @GetMapping("/key-value/{store}/{key}")
    public ResponseEntity<StateStoreRecord> getByKeyInKeyValueStore(@PathVariable("store") String store,
                                                                    @PathVariable("key") String key) {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(keyValueStoreService.getByKey(store, key));
    }

    /**
     * Get all records from the window store.
     *
     * @param store The store
     * @param timeFrom The time from
     * @param timeTo The time to
     * @return The values
     */
    @Operation(summary = "Get all records from a window store")
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
    @GetMapping(value = "/window/{store}")
    public ResponseEntity<List<StateStoreRecord>> getAllInWindowStore(@PathVariable("store") String store,
                                                                      @RequestParam("timeFrom")
                                                                        Optional<String> timeFrom,
                                                                      @RequestParam("timeTo") Optional<String> timeTo) {
        Instant instantFrom = timeFrom.map(Instant::parse).orElse(Instant.EPOCH);
        Instant instantTo = timeTo.map(Instant::parse).orElse(Instant.now());

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(windowStoreService.getAll(store, instantFrom, instantTo));
    }

    /**
     * Get all records from the window store on the local host.
     *
     * @param store The store
     * @param timeFrom The time from
     * @param timeTo The time to
     * @return The values
     */
    @Operation(summary = "Get all records from a window store on the local host")
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
    @GetMapping(value = "/window/local/{store}")
    public ResponseEntity<List<StateStoreRecord>> getAllInWindowStoreOnLocalHost(@PathVariable("store") String store,
                                                                                 @RequestParam("timeFrom")
                                                                                    Optional<String> timeFrom,
                                                                                 @RequestParam("timeTo")
                                                                                    Optional<String> timeTo) {
        Instant instantFrom = timeFrom.map(Instant::parse).orElse(Instant.EPOCH);
        Instant instantTo = timeTo.map(Instant::parse).orElse(Instant.now());

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(windowStoreService.getAllOnLocalHost(store, instantFrom, instantTo));
    }

    /**
     * Get the record by key from the window store.
     *
     * @param store The store
     * @param key The key
     * @param timeFrom The time from
     * @param timeTo The time to
     * @return The value
     */
    @Operation(summary = "Get a record by key from a window store")
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
    @GetMapping("/window/{store}/{key}")
    public ResponseEntity<List<StateStoreRecord>> getByKeyInWindowStore(@PathVariable("store") String store,
                                                                        @PathVariable("key") String key,
                                                                        @RequestParam("timeFrom")
                                                                            Optional<String> timeFrom,
                                                                        @RequestParam("timeTo")
                                                                            Optional<String> timeTo) {
        Instant instantFrom = timeFrom.map(Instant::parse).orElse(Instant.EPOCH);
        Instant instantTo = timeTo.map(Instant::parse).orElse(Instant.now());

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(windowStoreService.getByKey(store, key, instantFrom, instantTo));
    }
}
