package com.michelin.kstreamplify.controller;

import com.michelin.kstreamplify.http.service.KubernetesService;
import com.michelin.kstreamplify.http.service.RestResponse;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Kafka Streams controller for Kubernetes.
 */
@RestController
@ConditionalOnBean(KafkaStreamsStarter.class)
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
    @GetMapping("/${readiness_path:ready}")
    public ResponseEntity<Void> readiness() {
        RestResponse<Void> response = kubernetesService.getReadiness();
        return ResponseEntity
            .status(response.getStatus())
            .body(response.getBody());
    }

    /**
     * Liveness Kubernetes probe endpoint.
     *
     * @return An HTTP response based on the Kafka Streams state
     */
    @GetMapping("/${liveness_path:liveness}")
    public ResponseEntity<Void> liveness() {
        RestResponse<Void> response = kubernetesService.getLiveness();
        return ResponseEntity
            .status(response.getStatus())
            .body(response.getBody());
    }
}
