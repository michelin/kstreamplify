package com.michelin.kstreamplify.topology;

import static com.michelin.kstreamplify.util.RestUtils.toResponseEntity;

import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.initializer.SpringBootKafkaStreamsInitializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
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
     * The Kafka Streams initializer.
     */
    @Autowired
    private SpringBootKafkaStreamsInitializer kafkaStreamsInitializer;

    /**
     * Get the Kafka Streams topology.
     *
     * @return The Kafka Streams topology
     */
    @GetMapping("/${topology_path:topology}")
    public ResponseEntity<String> topology() {
        return toResponseEntity(TopologyService.getTopology(kafkaStreamsInitializer));
    }
}
