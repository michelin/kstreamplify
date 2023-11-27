package com.michelin.kstreamplify.services;

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
 * Kafka Streams probe service.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ProbeService {
    /**
     * Kubernetes' readiness probe.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     * @return An HTTP response based on the Kafka Streams state
     */
    public static RestServiceResponse<String> readinessProbe(
        KafkaStreamsInitializer kafkaStreamsInitializer) {
        if (kafkaStreamsInitializer.getKafkaStreams() != null) {
            log.debug("Kafka Stream \"{}\" state: {}",
                KafkaStreamsExecutionContext.getProperties()
                    .getProperty(StreamsConfig.APPLICATION_ID_CONFIG),
                kafkaStreamsInitializer.getKafkaStreams().state());

            if (kafkaStreamsInitializer.getKafkaStreams().state()
                == KafkaStreams.State.REBALANCING) {
                long startingThreadCount =
                    kafkaStreamsInitializer.getKafkaStreams().metadataForLocalThreads()
                        .stream()
                        .filter(t -> StreamThread.State.STARTING.name()
                            .compareToIgnoreCase(t.threadState()) == 0
                            || StreamThread.State.CREATED.name()
                            .compareToIgnoreCase(t.threadState()) == 0)
                        .count();

                if (startingThreadCount
                    == kafkaStreamsInitializer.getKafkaStreams().metadataForLocalThreads().size()) {
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
    public static RestServiceResponse<String> livenessProbe(
        KafkaStreamsInitializer kafkaStreamsInitializer) {
        if (kafkaStreamsInitializer.getKafkaStreams() != null) {
            return kafkaStreamsInitializer.getKafkaStreams().state()
                != KafkaStreams.State.NOT_RUNNING
                ? RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_OK).build()
                :
                RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_INTERNAL_ERROR)
                    .build();
        }
        return RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_NO_CONTENT)
            .build();
    }

    /**
     * Get the Kafka Streams topology.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     * @return The Kafka Streams topology
     */
    public static RestServiceResponse<String> exposeTopology(
        KafkaStreamsInitializer kafkaStreamsInitializer) {
        if (kafkaStreamsInitializer.getTopology() != null) {
            return RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_OK)
                .body(kafkaStreamsInitializer.getTopology().describe().toString()).build();
        }
        return RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_NO_CONTENT)
            .build();
    }
}
