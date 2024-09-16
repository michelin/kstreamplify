package com.michelin.kstreamplify.controller;

import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.service.TopologyService;
import io.swagger.v3.oas.annotations.Operation;
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
 * Kafka Streams controller for topology.
 */
@RestController
@ConditionalOnBean(KafkaStreamsStarter.class)
@Tag(name = "Topology", description = "Topology Controller")
public class TopologyController {
    /**
     * The topology service.
     */
    @Autowired
    private TopologyService topologyService;

    /**
     * Get the Kafka Streams topology.
     *
     * @return The Kafka Streams topology
     */
    @Operation(summary = "Get the Kafka Streams topology")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "OK")
    })
    @GetMapping("/${topology.path:topology}")
    public ResponseEntity<String> topology() {
        return ResponseEntity
            .ok()
            .contentType(MediaType.TEXT_PLAIN)
            .body(topologyService.getTopology());
    }
}
