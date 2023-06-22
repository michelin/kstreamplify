package io.github.michelin.spring.kafka.streams.rest;

import io.github.michelin.spring.kafka.streams.initializer.KafkaStreamsStarter;
import io.github.michelin.spring.kafka.streams.initializer.SpringKafkaStreamsInitializer;
import io.github.michelin.spring.kafka.streams.model.RestServiceResponse;
import io.github.michelin.spring.kafka.streams.services.ProbeService;
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

    @GetMapping("/${topology_path:topology}")
    public ResponseEntity<String> exposeTopology() {
        return convertToResponseEntity(ProbeService.exposeTopology(kafkaStreamsInitializer));
    }

    private static ResponseEntity<String> convertToResponseEntity(RestServiceResponse<String> serviceResponse) {
        return ResponseEntity.status(serviceResponse.getStatus()).body(serviceResponse.getBody());
    }
}
