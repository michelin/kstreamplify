package com.michelin.kstreamplify.rest;

import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.initializer.SpringKafkaStreamsInitializer;
import com.michelin.kstreamplify.model.RestServiceResponse;
import com.michelin.kstreamplify.services.ProbeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Spring Boot probe controller.
 */
@RestController
@ConditionalOnBean(KafkaStreamsStarter.class)
public class SpringProbeController {
    /**
     * The Kafka Streams initializer.
     */
    @Autowired
    private SpringKafkaStreamsInitializer kafkaStreamsInitializer;

    /**
     * Readiness Kubernetes probe endpoint.
     *
     * @return An HTTP response based on the Kafka Streams state
     */
    @GetMapping("/${readiness_path:ready}")
    public ResponseEntity<String> readinessProbe() {
        return convertToResponseEntity(ProbeService.readinessProbe(kafkaStreamsInitializer));
    }

    /**
     * Liveness Kubernetes probe endpoint.
     *
     * @return An HTTP response based on the Kafka Streams state
     */
    @GetMapping("/${liveness_path:liveness}")
    public ResponseEntity<String> livenessProbe() {
        return convertToResponseEntity(ProbeService.livenessProbe(kafkaStreamsInitializer));
    }

    /**
     * Get the Kafka Streams topology.
     *
     * @return The Kafka Streams topology
     */
    @GetMapping("/${expose_topology_path:topology}")
    public ResponseEntity<String> exposeTopology() {
        return convertToResponseEntity(ProbeService.exposeTopology(kafkaStreamsInitializer));
    }

    /**
     * Convert the probe service response into an HTTP response entity.
     *
     * @param serviceResponse The probe service response
     * @return An HTTP response
     */
    private static ResponseEntity<String> convertToResponseEntity(
        RestServiceResponse<String> serviceResponse) {
        return ResponseEntity.status(serviceResponse.getStatus()).body(serviceResponse.getBody());
    }
}
