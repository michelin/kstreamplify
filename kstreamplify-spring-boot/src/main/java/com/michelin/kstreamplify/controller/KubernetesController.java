package com.michelin.kstreamplify.controller;

import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.service.KubernetesService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Kafka Streams controller for Kubernetes.
 */
@RestController
@ConditionalOnBean(KafkaStreamsStarter.class)
@Tag(name = "Kubernetes", description = "Kubernetes Controller")
public class KubernetesController {
    /**
     * The Kubernetes service.
     */
    @Autowired
    private KubernetesService kubernetesService;

    /**
     * Readiness Kubernetes probe endpoint.
     *
     * @return An HTTP response based on the Kafka Streams state
     */
    @Operation(summary = "Kubernetes readiness probe")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Kafka Streams running"),
        @ApiResponse(responseCode = "204", description = "Kafka Streams starting", content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = String.class))
        }),
        @ApiResponse(responseCode = "400", description = "Kafka Streams not instantiated", content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = String.class))
        }),
        @ApiResponse(responseCode = "503", description = "Kafka Streams not running", content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = String.class))
        }),
    })
    @GetMapping("/${kubernetes.readiness.path:ready}")
    public ResponseEntity<Void> readiness() {
        int readinessStatus = kubernetesService.getReadiness();
        return ResponseEntity
            .status(readinessStatus)
            .build();
    }

    /**
     * Liveness Kubernetes probe endpoint.
     *
     * @return An HTTP response based on the Kafka Streams state
     */
    @Operation(summary = "Kubernetes liveness probe")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Kafka Streams running"),
        @ApiResponse(responseCode = "400", description = "Kafka Streams not instantiated", content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = String.class))
        }),
        @ApiResponse(responseCode = "503", description = "Kafka Streams not running", content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = String.class))
        }),
    })
    @GetMapping("/${kubernetes.liveness.path:liveness}")
    public ResponseEntity<Void> liveness() {
        int livenessStatus = kubernetesService.getLiveness();
        return ResponseEntity
            .status(livenessStatus)
            .build();
    }
}
