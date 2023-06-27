package io.github.michelin.kstreamplify.services;

import io.github.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import io.github.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import io.github.michelin.kstreamplify.model.RestServiceResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.StreamThread;

import java.net.HttpURLConnection;

@Slf4j
public abstract class ProbeService {

    /**
     * The Kubernetes readiness probe endpoint
     *
     * @return An HTTP response based on the Kafka Streams state
     */
    public static RestServiceResponse<String> readinessProbe(KafkaStreamsInitializer kafkaStreamsInitializer) {
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
                    return RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_NO_CONTENT).build();
                }
            }

            return kafkaStreamsInitializer.getKafkaStreams().state().isRunningOrRebalancing() ?
                    RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_OK).build() : RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_UNAVAILABLE).build();
        }

        return RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_BAD_REQUEST).build();
    }

    /**
     * The Kubernetes liveness probe endpoint
     *
     * @return An HTTP response based on the Kafka Streams state
     */
    public static RestServiceResponse<String> livenessProbe(KafkaStreamsInitializer kafkaStreamsInitializer) {
        if (kafkaStreamsInitializer.getKafkaStreams() != null) {
            return kafkaStreamsInitializer.getKafkaStreams().state() != KafkaStreams.State.NOT_RUNNING ? RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_OK).build()
                    : RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_INTERNAL_ERROR).build();
        }

        return RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_NO_CONTENT).build();
    }

    /**
     * The topology endpoint
     *
     * @return The Kafka Streams topology as JSON
     */
    public static RestServiceResponse<String> exposeTopology(KafkaStreamsInitializer kafkaStreamsInitializer) {
        if (kafkaStreamsInitializer.getTopology() != null) {
            return RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_OK).body(kafkaStreamsInitializer.getTopology().describe().toString()).build();
        }
        return RestServiceResponse.<String>builder().status(HttpURLConnection.HTTP_NO_CONTENT).build();
    }
}
