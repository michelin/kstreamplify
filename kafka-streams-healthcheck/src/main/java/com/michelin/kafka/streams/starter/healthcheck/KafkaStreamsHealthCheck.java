package com.michelin.kafka.streams.starter.healthcheck;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class KafkaStreamsHealthCheck {
    @Autowired
    private KafkaStreams kafkaStreams;

    @GetMapping("ready")
    public ResponseEntity<String> readinessProbe() {
        if (kafkaStreams != null) {
            log.debug("Kafka Stream current state: " + kafkaStreams.state().toString());

            if (kafkaStreams.state() == KafkaStreams.State.REBALANCING) {
                long startingThreadCount = kafkaStreams.metadataForLocalThreads()
                        .stream()
                        .filter(t -> StreamThread.State.STARTING.name().compareToIgnoreCase(t.threadState()) == 0 || StreamThread.State.CREATED.name().compareToIgnoreCase(t.threadState()) == 0)
                        .count();

                if (startingThreadCount == kafkaStreams.metadataForLocalThreads().size()) {
                    return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
                }
            }

            return kafkaStreams.state().isRunningOrRebalancing() ?
                    ResponseEntity.ok().build() : ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }

        return ResponseEntity.badRequest().build();
    }

    @GetMapping("liveness")
    public ResponseEntity<String> livenessProbe() {
        if (kafkaStreams != null) {
            return kafkaStreams.state() != KafkaStreams.State.NOT_RUNNING ? ResponseEntity.ok().build()
                    : ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }

        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }
}
