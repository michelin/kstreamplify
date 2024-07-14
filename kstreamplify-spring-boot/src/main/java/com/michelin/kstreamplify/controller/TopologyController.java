package com.michelin.kstreamplify.controller;

import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.service.TopologyService;
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
    @GetMapping("/${topology.path:topology}")
    public ResponseEntity<String> topology() {
        return ResponseEntity
            .ok()
            .contentType(MediaType.TEXT_PLAIN)
            .body(topologyService.getTopology());
    }
}
