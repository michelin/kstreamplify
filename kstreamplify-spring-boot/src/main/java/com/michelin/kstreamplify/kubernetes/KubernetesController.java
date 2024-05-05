package com.michelin.kstreamplify.kubernetes;

import static com.michelin.kstreamplify.util.RestUtils.toResponseEntity;

import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.initializer.SpringBootKafkaStreamsInitializer;
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
     * The Kafka Streams initializer.
     */
    @Autowired
    private SpringBootKafkaStreamsInitializer kafkaStreamsInitializer;

    /**
     * Readiness Kubernetes probe endpoint.
     *
     * @return An HTTP response based on the Kafka Streams state
     */
    @GetMapping("/${readiness_path:ready}")
    public ResponseEntity<Void> readiness() {
        return toResponseEntity(KubernetesService.getReadiness(kafkaStreamsInitializer));
    }

    /**
     * Liveness Kubernetes probe endpoint.
     *
     * @return An HTTP response based on the Kafka Streams state
     */
    @GetMapping("/${liveness_path:liveness}")
    public ResponseEntity<Void> liveness() {
        return toResponseEntity(KubernetesService.getLiveness(kafkaStreamsInitializer));
    }
}
