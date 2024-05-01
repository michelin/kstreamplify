package com.michelin.kstreamplify.kubernetes;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.model.RestServiceResponse;
import java.net.HttpURLConnection;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.StreamThread;

/**
 * Kafka Streams Kubernetes service.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class KubernetesService {
    public static final String READINESS_PATH_PROPERTY_NAME = "readiness_path";
    public static final String LIVENESS_PATH_PROPERTY_NAME = "liveness_path";
    public static final String DEFAULT_READINESS_PATH = "ready";
    public static final String DEFAULT_LIVENESS_PATH = "liveness";

    /**
     * Kubernetes' readiness probe.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     * @return An HTTP response based on the Kafka Streams state
     */
    public static RestServiceResponse<String> getReadiness(KafkaStreamsInitializer kafkaStreamsInitializer) {
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
                    return RestServiceResponse.<String>builder()
                        .status(HttpURLConnection.HTTP_NO_CONTENT).build();
                }
            }

            return kafkaStreamsInitializer.getKafkaStreams().state().equals(KafkaStreams.State.RUNNING)
                ? RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_OK).build() :
                RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_UNAVAILABLE)
                    .build();
        }
        return RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_BAD_REQUEST)
            .build();
    }

    /**
     * Kubernetes' liveness probe.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     * @return An HTTP response based on the Kafka Streams state
     */
    public static RestServiceResponse<String> getLiveness(KafkaStreamsInitializer kafkaStreamsInitializer) {
        if (kafkaStreamsInitializer.getKafkaStreams() != null) {
            return kafkaStreamsInitializer.getKafkaStreams().state() != KafkaStreams.State.NOT_RUNNING
                ? RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_OK).build()
                : RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_INTERNAL_ERROR)
                .build();
        }
        return RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_NO_CONTENT)
            .build();
    }


}
