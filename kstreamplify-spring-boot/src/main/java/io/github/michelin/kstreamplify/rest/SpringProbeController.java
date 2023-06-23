package io.github.michelin.kstreamplify.rest;

import io.github.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import io.github.michelin.kstreamplify.model.RestServiceResponse;
import io.github.michelin.kstreamplify.services.ProbeService;
import io.github.michelin.kstreamplify.initializer.SpringKafkaStreamsInitializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ConditionalOnBean(KafkaStreamsStarter.class)
public class SpringProbeController {

    @Autowired
    SpringKafkaStreamsInitializer kafkaStreamsInitializer;

    @GetMapping("/${readiness_path:ready}")
    public ResponseEntity<String> readinessProbe() {
        return convertToResponseEntity(ProbeService.readinessProbe(kafkaStreamsInitializer));
    }

    @GetMapping("/${liveness_path:liveness}")
    public ResponseEntity<String> livenessProbe() {
        return convertToResponseEntity(ProbeService.livenessProbe(kafkaStreamsInitializer));
    }

    @GetMapping("/${expose_topology_path:topology}")
    public ResponseEntity<String> exposeTopology() {
        return convertToResponseEntity(ProbeService.exposeTopology(kafkaStreamsInitializer));
    }

    private static ResponseEntity<String> convertToResponseEntity(RestServiceResponse<String> serviceResponse) {
        return ResponseEntity.status(serviceResponse.getStatus()).body(serviceResponse.getBody());
    }
}
