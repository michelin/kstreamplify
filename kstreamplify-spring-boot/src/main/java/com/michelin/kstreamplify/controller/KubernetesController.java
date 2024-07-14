package com.michelin.kstreamplify.controller;

import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.service.KubernetesService;
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
    @GetMapping("/${kubernetes.liveness.path:liveness}")
    public ResponseEntity<Void> liveness() {
        int livenessStatus = kubernetesService.getLiveness();
        return ResponseEntity
            .status(livenessStatus)
            .build();
    }
}
