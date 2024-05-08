package com.michelin.kstreamplify.http.service;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import java.net.HttpURLConnection;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.StreamThread;

/**
 * Kafka Streams Kubernetes service.
 */
@Slf4j
@AllArgsConstructor
public final class KubernetesService {
    /**
     * The readiness path property name.
     */
    public static final String READINESS_PATH_PROPERTY_NAME = "kubernetes.readiness-path";

    /**
     * The liveness path property name.
     */
    public static final String LIVENESS_PATH_PROPERTY_NAME = "kubernetes.liveness-path";

    /**
     * The default readiness path.
     */
    public static final String DEFAULT_READINESS_PATH = "ready";

    /**
     * The default liveness path.
     */
    public static final String DEFAULT_LIVENESS_PATH = "liveness";

    /**
     * The Kafka Streams initializer.
     */
    private final KafkaStreamsInitializer kafkaStreamsInitializer;

    /**
     * Kubernetes' readiness probe.
     *
     * @return An HTTP response based on the Kafka Streams state
     */
    public RestResponse<Void> getReadiness() {
        if (kafkaStreamsInitializer.getKafkaStreams() != null) {
            log.debug("Kafka Stream \"{}\" state: {}",
                KafkaStreamsExecutionContext.getProperties().getProperty(StreamsConfig.APPLICATION_ID_CONFIG),
                kafkaStreamsInitializer.getKafkaStreams().state());

            if (kafkaStreamsInitializer.getKafkaStreams().state() == KafkaStreams.State.REBALANCING) {
                long startingThreadCount = kafkaStreamsInitializer.getKafkaStreams().metadataForLocalThreads()
                    .stream()
                    .filter(t -> StreamThread.State.STARTING.name()
                        .compareToIgnoreCase(t.threadState()) == 0 || StreamThread.State.CREATED.name()
                        .compareToIgnoreCase(t.threadState()) == 0)
                    .count();

                if (startingThreadCount == kafkaStreamsInitializer.getKafkaStreams().metadataForLocalThreads().size()) {
                    return RestResponse.<Void>builder()
                        .status(HttpURLConnection.HTTP_NO_CONTENT).build();
                }
            }

            return kafkaStreamsInitializer.getKafkaStreams().state().equals(KafkaStreams.State.RUNNING)
                ? RestResponse.<Void>builder().status(HttpURLConnection.HTTP_OK).build() :
                RestResponse.<Void>builder().status(HttpURLConnection.HTTP_UNAVAILABLE)
                    .build();
        }
        return RestResponse.<Void>builder().status(HttpURLConnection.HTTP_BAD_REQUEST)
            .build();
    }

    /**
     * Kubernetes' liveness probe.
     *
     * @return An HTTP response based on the Kafka Streams state
     */
    public RestResponse<Void> getLiveness() {
        if (kafkaStreamsInitializer.getKafkaStreams() != null) {
            return kafkaStreamsInitializer.getKafkaStreams().state() != KafkaStreams.State.NOT_RUNNING
                ? RestResponse.<Void>builder().status(HttpURLConnection.HTTP_OK).build()
                : RestResponse.<Void>builder().status(HttpURLConnection.HTTP_INTERNAL_ERROR)
                .build();
        }
        return RestResponse.<Void>builder().status(HttpURLConnection.HTTP_NO_CONTENT)
            .build();
    }
}
