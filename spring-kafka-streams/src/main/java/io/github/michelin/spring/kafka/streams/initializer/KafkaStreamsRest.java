package io.github.michelin.spring.kafka.streams.initializer;

import io.github.michelin.spring.kafka.streams.model.TopologyExposeJsonModel;
import io.github.michelin.spring.kafka.streams.properties.KafkaProperties;
import io.github.michelin.spring.kafka.streams.services.ConvertTopology;
import io.github.michelin.spring.kafka.streams.context.KafkaStreamsExecutionContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * The Kafka Streams endpoints class
 */
@Slf4j
@RestController
@ConditionalOnBean(KafkaStreamsStarter.class)
public class KafkaStreamsRest {
    /**
     * The application context
     */
    @Autowired
    protected ConfigurableApplicationContext applicationContext;

    /**
     * The Kafka properties
     */
    @Autowired
    protected KafkaProperties kafkaProperties;

    /**
     * The Kafka Streams initializer
     */
    @Autowired
    private KafkaStreamsInitializer kafkaStreamsInitializer;

    /**
     * The Kubernetes readiness probe endpoint
     * @return An HTTP response based on the Kafka Streams state
     */
    @GetMapping("ready")
    public ResponseEntity<String> readinessProbe() {
        if (kafkaStreamsInitializer.getKafkaStreams() != null) {
            log.debug("Kafka Stream \"{}\" state: {}",
                    KafkaStreamsExecutionContext.getProperties().getProperty(StreamsConfig.APPLICATION_ID_CONFIG),
                    kafkaStreamsInitializer.getKafkaStreams().state());

            if (kafkaStreamsInitializer.getKafkaStreams().state() == KafkaStreams.State.REBALANCING) {
                long startingThreadCount = kafkaStreamsInitializer.getKafkaStreams().metadataForLocalThreads()
                        .stream()
                        .filter(t -> StreamThread.State.STARTING.name().compareToIgnoreCase(t.threadState()) == 0 || StreamThread.State.CREATED.name().compareToIgnoreCase(t.threadState()) == 0)
                        .count();

                if (startingThreadCount == kafkaStreamsInitializer.getKafkaStreams().metadataForLocalThreads().size()) {
                    return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
                }
            }

            return kafkaStreamsInitializer.getKafkaStreams().state().isRunningOrRebalancing() ?
                    ResponseEntity.ok().build() : ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }

        return ResponseEntity.badRequest().build();
    }

    /**
     * The Kubernetes liveness probe endpoint
     * @return An HTTP response based on the Kafka Streams state
     */
    @GetMapping("liveness")
    public ResponseEntity<String> livenessProbe() {
        if (kafkaStreamsInitializer.getKafkaStreams() != null) {
            return kafkaStreamsInitializer.getKafkaStreams().state() != KafkaStreams.State.NOT_RUNNING ? ResponseEntity.ok().build()
                    : ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }

        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }

    /**
     * The topology endpoint
     * @return The Kafka Streams topology as JSON
     */
    @GetMapping("topology")
    public ResponseEntity<TopologyExposeJsonModel> exposeTopology() {
        if (kafkaStreamsInitializer.getTopology() != null) {
            return ResponseEntity.ok(ConvertTopology.convertTopologyForRest(kafkaProperties.getProperties().get(StreamsConfig.APPLICATION_ID_CONFIG),
                    kafkaStreamsInitializer.getTopology()));
        }

        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }

    /**
     * The state store clean up endpoint
     * @param debounceParam The delay in seconds
     * @return An HTTP response
     */
    @GetMapping("shutdown/{debounceParam}")
    public ResponseEntity<String> shutdown(@PathVariable String debounceParam) {
        long debounce = 1L;

        if (!StringUtils.isBlank(debounceParam)) {
            debounce = Long.parseLong(debounceParam);
        }

        if (kafkaStreamsInitializer.getKafkaStreams() != null) {
            log.info("Clean up and close in {}s", debounce);
            kafkaStreamsInitializer.getKafkaStreams().close();
            try {
                kafkaStreamsInitializer.getKafkaStreams().cleanUp();
            } catch (IllegalStateException ex) {
                log.warn("Error on state dir clean up", ex);
            }

            long shutdownDebounce = debounce * 1000L;
            var springClose = new Thread(() -> {
                try {
                    log.info("Shutdown scheduled in {}", shutdownDebounce);
                    Thread.sleep(shutdownDebounce);
                } catch (InterruptedException e) {
                    log.warn("Error while waiting for stop", e);
                    Thread.currentThread().interrupt();
                }

                if (applicationContext != null) {
                    applicationContext.close();
                } else{
                    log.warn("No Spring context set");
                }
            });

            springClose.start();
        }

        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }
}
