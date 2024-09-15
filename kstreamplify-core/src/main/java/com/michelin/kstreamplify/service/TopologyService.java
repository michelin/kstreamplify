package com.michelin.kstreamplify.service;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Kafka Streams topology service.
 */
@Slf4j
@AllArgsConstructor
public class TopologyService {
    /**
     * The topology path property.
     */
    public static final String TOPOLOGY_PATH_PROPERTY_NAME = "topology.path";

    /**
     * The default topology path.
     */
    public static final String TOPOLOGY_DEFAULT_PATH = "topology";

    /**
     * The Kafka Streams initializer.
     */
    private final KafkaStreamsInitializer kafkaStreamsInitializer;

    /**
     * Get the Kafka Streams topology.
     *
     * @return The Kafka Streams topology
     */
    public String getTopology() {
        return kafkaStreamsInitializer.getTopology().describe().toString();
    }
}
