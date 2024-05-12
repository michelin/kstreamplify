package com.michelin.kstreamplify.service;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.model.RestResponse;
import java.net.HttpURLConnection;
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
    public static final String TOPOLOGY_PROPERTY = "topology.topology-path";

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
    public RestResponse<String> getTopology() {
        if (kafkaStreamsInitializer.getTopology() != null) {
            return RestResponse.<String>builder().status(HttpURLConnection.HTTP_OK)
                .body(kafkaStreamsInitializer.getTopology().describe().toString()).build();
        }
        return RestResponse.<String>builder().status(HttpURLConnection.HTTP_NO_CONTENT)
            .build();
    }
}
