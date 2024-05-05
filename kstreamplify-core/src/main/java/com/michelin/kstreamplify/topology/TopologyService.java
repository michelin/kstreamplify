package com.michelin.kstreamplify.topology;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.model.RestServiceResponse;
import java.net.HttpURLConnection;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Kafka Streams topology service.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TopologyService {
    /**
     * The topology path property.
     */
    public static final String TOPOLOGY_PROPERTY = "topology_path";

    /**
     * The default topology path.
     */
    public static final String TOPOLOGY_DEFAULT_PATH = "topology";

    /**
     * Get the Kafka Streams topology.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     * @return The Kafka Streams topology
     */
    public static RestServiceResponse<String> getTopology(KafkaStreamsInitializer kafkaStreamsInitializer) {
        if (kafkaStreamsInitializer.getTopology() != null) {
            return RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_OK)
                .body(kafkaStreamsInitializer.getTopology().describe().toString()).build();
        }
        return RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_NO_CONTENT)
            .build();
    }
}
