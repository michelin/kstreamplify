/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.michelin.kstreamplify.controller;

import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.service.interactivequeries.keyvalue.KeyValueStoreService;
import com.michelin.kstreamplify.service.interactivequeries.keyvalue.TimestampedKeyValueStoreService;
import com.michelin.kstreamplify.service.interactivequeries.window.TimestampedWindowStoreService;
import com.michelin.kstreamplify.service.interactivequeries.window.WindowStoreService;
import com.michelin.kstreamplify.store.StateStoreRecord;
import com.michelin.kstreamplify.store.StreamsMetadata;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
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
    private KeyValueStoreService keyValueService;

    @Autowired
    private TimestampedKeyValueStoreService timestampedKeyValueService;

    @Autowired
    private WindowStoreService windowStoreService;

    @Autowired
    private TimestampedWindowStoreService timestampedWindowStoreService;

    /**
     * Get the stores.
     *
     * @return The stores
     */
    @Operation(summary = "Get the state stores.")
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
            .body(keyValueService.getStateStores());
    }

    /**
     * Get the hosts of the store.
     *
     * @param store The store
     * @return The hosts
     */
    @Operation(summary = "Get the streams metadata for a store.")
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
            .body(keyValueService.getStreamsMetadataForStore(store)
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
    @Operation(summary = "Get all records from a key-value store.")
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
            .body(keyValueService.getAll(store));
    }

    /**
     * Get all records from the key-value store on the local instance.
     *
     * @param store The store
     * @return The values
     */
    @Operation(summary = "Get all records from a key-value store on the local instance.")
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
    public ResponseEntity<List<StateStoreRecord>> getAllInKeyValueStoreOnLocalHost(
        @PathVariable("store") String store) {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(keyValueService.getAllOnLocalInstance(store));
    }

    /**
     * Get the record by key from the key-value store.
     *
     * @param store The store
     * @param key   The key
     * @return The value
     */
    @Operation(summary = "Get a record by key from a key-value store.")
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
    public ResponseEntity<StateStoreRecord> getByKeyInKeyValueStore(
        @PathVariable("store") String store,
        @PathVariable("key") String key) {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(keyValueService.getByKey(store, key));
    }

    /**
     * Get all records from the timestamped key-value store.
     *
     * @param store The store
     * @return The values
     */
    @Operation(summary = "Get all records from a timestamped key-value store.")
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
    @GetMapping(value = "/key-value/timestamped/{store}")
    public ResponseEntity<List<StateStoreRecord>> getAllInTimestampedKeyValueStore(
        @PathVariable("store") String store) {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(timestampedKeyValueService.getAll(store));
    }

    /**
     * Get all records from the timestamped key-value store on the local instance.
     *
     * @param store The store
     * @return The values
     */
    @Operation(summary = "Get all records from a timestamped key-value store on the local instance.")
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
    @GetMapping(value = "/key-value/timestamped/local/{store}")
    public ResponseEntity<List<StateStoreRecord>> getAllInTimestampedKeyValueStoreOnLocalHost(
        @PathVariable("store") String store) {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(timestampedKeyValueService.getAllOnLocalInstance(store));
    }

    /**
     * Get the record by key from the timestamped key-value store.
     *
     * @param store The store
     * @param key   The key
     * @return The value
     */
    @Operation(summary = "Get a record by key from a timestamped key-value store.")
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
    @GetMapping("/key-value/timestamped/{store}/{key}")
    public ResponseEntity<StateStoreRecord> getByKeyInTimestampedKeyValueStore(
        @PathVariable("store") String store,
        @PathVariable("key") String key) {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(timestampedKeyValueService.getByKey(store, key));
    }

    /**
     * Get all records from the window store.
     *
     * @param store    The store
     * @param startTime The start time
     * @param endTime   The end time
     * @return The values
     */
    @Operation(summary = "Get all records from a window store.")
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
    public ResponseEntity<List<StateStoreRecord>> getAllInWindowStore(
        @PathVariable("store") String store,
        @Parameter(description = "1970-01-01T01:01:01Z") @RequestParam("startTime") Optional<String> startTime,
        @Parameter(description = "1970-01-01T01:01:01Z") @RequestParam("endTime") Optional<String> endTime) {
        Instant instantFrom = startTime.map(Instant::parse).orElse(Instant.EPOCH);
        Instant instantTo = endTime.map(Instant::parse).orElse(Instant.now());

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(windowStoreService.getAll(store, instantFrom, instantTo));
    }

    /**
     * Get all records from the window store on the local instance.
     *
     * @param store    The store
     * @param startTime The start time
     * @param endTime   The end time
     * @return The values
     */
    @Operation(summary = "Get all records from a window store on the local instance.")
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
    public ResponseEntity<List<StateStoreRecord>> getAllInWindowStoreOnLocalHost(
        @PathVariable("store") String store,
        @Parameter(description = "1970-01-01T01:01:01Z") @RequestParam("startTime") Optional<String> startTime,
        @Parameter(description = "1970-01-01T01:01:01Z") @RequestParam("endTime") Optional<String> endTime) {
        Instant instantFrom = startTime.map(Instant::parse).orElse(Instant.EPOCH);
        Instant instantTo = endTime.map(Instant::parse).orElse(Instant.now());

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(windowStoreService.getAllOnLocalInstance(store, instantFrom, instantTo));
    }

    /**
     * Get the record by key from the window store.
     *
     * @param store    The store
     * @param key      The key
     * @param startTime The start time
     * @param endTime   The end time
     * @return The value
     */
    @Operation(summary = "Get a record by key from a window store.")
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
    public ResponseEntity<List<StateStoreRecord>> getByKeyInWindowStore(
        @PathVariable("store") String store,
        @PathVariable("key") String key,
        @Parameter(description = "1970-01-01T01:01:01Z") @RequestParam("startTime") Optional<String> startTime,
        @Parameter(description = "1970-01-01T01:01:01Z") @RequestParam("endTime") Optional<String> endTime) {
        Instant instantFrom = startTime.map(Instant::parse).orElse(Instant.EPOCH);
        Instant instantTo = endTime.map(Instant::parse).orElse(Instant.now());

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(windowStoreService.getByKey(store, key, instantFrom, instantTo));
    }

    /**
     * Get all records from the timestamped window store.
     *
     * @param store    The store
     * @param startTime The start time
     * @param endTime   The end time
     * @return The values
     */
    @Operation(summary = "Get all records from a timestamped window store.")
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
    @GetMapping(value = "/window/timestamped/{store}")
    public ResponseEntity<List<StateStoreRecord>> getAllInTimestampedWindowStore(
        @PathVariable("store") String store,
        @Parameter(description = "1970-01-01T01:01:01Z") @RequestParam("startTime") Optional<String> startTime,
        @Parameter(description = "1970-01-01T01:01:01Z") @RequestParam("endTime") Optional<String> endTime) {
        Instant instantFrom = startTime.map(Instant::parse).orElse(Instant.EPOCH);
        Instant instantTo = endTime.map(Instant::parse).orElse(Instant.now());

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(timestampedWindowStoreService.getAll(store, instantFrom, instantTo));
    }

    /**
     * Get all records from the timestamped window store on the local instance.
     *
     * @param store    The store
     * @param startTime The start time
     * @param endTime   The end time
     * @return The values
     */
    @Operation(summary = "Get all records from a timestamped window store on the local instance.")
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
    @GetMapping(value = "/window/timestamped/local/{store}")
    public ResponseEntity<List<StateStoreRecord>> getAllInTimestampedWindowStoreOnLocalHost(
        @PathVariable("store") String store,
        @Parameter(description = "1970-01-01T01:01:01Z") @RequestParam("startTime") Optional<String> startTime,
        @Parameter(description = "1970-01-01T01:01:01Z") @RequestParam("endTime") Optional<String> endTime) {
        Instant instantFrom = startTime.map(Instant::parse).orElse(Instant.EPOCH);
        Instant instantTo = endTime.map(Instant::parse).orElse(Instant.now());

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(timestampedWindowStoreService.getAllOnLocalInstance(store, instantFrom, instantTo));
    }

    /**
     * Get the record by key from the timestamped window store.
     *
     * @param store    The store
     * @param key      The key
     * @param startTime The start time
     * @param endTime   The end time
     * @return The value
     */
    @Operation(summary = "Get a record by key from a timestamped window store.")
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
    @GetMapping("/window/timestamped/{store}/{key}")
    public ResponseEntity<List<StateStoreRecord>> getByKeyInTimestampedWindowStore(
        @PathVariable("store") String store,
        @PathVariable("key") String key,
        @Parameter(description = "1970-01-01T01:01:01Z") @RequestParam("startTime") Optional<String> startTime,
        @Parameter(description = "1970-01-01T01:01:01Z") @RequestParam("endTime") Optional<String> endTime) {
        Instant instantFrom = startTime.map(Instant::parse).orElse(Instant.EPOCH);
        Instant instantTo = endTime.map(Instant::parse).orElse(Instant.now());

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(timestampedWindowStoreService.getByKey(store, key, instantFrom, instantTo));
    }
}
