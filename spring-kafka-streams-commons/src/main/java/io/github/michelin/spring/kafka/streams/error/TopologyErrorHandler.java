package io.github.michelin.spring.kafka.streams.error;

import io.github.michelin.spring.kafka.streams.context.KafkaStreamsExecutionContext;
import io.github.michelin.spring.kafka.streams.utils.SerdesUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;

import java.util.Map;

/**
 * The topology error handler class
 */
@Slf4j
public class TopologyErrorHandler {
    private static final String BRANCHING_NAME_NOMINAL = "branch-nominal";

    private TopologyErrorHandler() { }

    /**
     * Catch the errors from the given stream
     * @param stream The stream of processing result that may contain processing errors
     * @return A stream filtered from all processing errors
     * @param <K> The key type
     * @param <V> The type of the successful record
     * @param <V2> The type of the failed record
     */
    public static <K, V, V2> KStream<K,V> catchErrors(KStream<K, ProcessingResult<V, V2>> stream) {
        return catchErrors(stream, false);
    }

    /**
     * Catch the errors from the given stream
     * @param stream The stream of processing result that may contain processing errors
     * @param allowTombstone Allow sending tombstone in DLQ topic or to be returned
     * @return A stream filtered from all processing errors
     * @param <K> The key type
     * @param <V> The type of the successful record
     * @param <V2> The type of the failed record
     */
    public static <K, V, V2> KStream<K,V> catchErrors(KStream<K, ProcessingResult<V, V2>> stream, boolean allowTombstone) {
        Map<String, KStream<K,ProcessingResult<V, V2>>> branches;

        String branchNamePrefix = stream.toString().split("@")[1];
        if (!allowTombstone) {
            branches = stream
                    .filter((key, value) -> value != null)
                    .filterNot((key, value) -> value.getValue() == null && value.getError() == null)
                    .split(Named.as(branchNamePrefix))
                    .branch((key, value) -> value.isValid(), Branched.as(BRANCHING_NAME_NOMINAL))
                    .defaultBranch(Branched.withConsumer(ks -> TopologyErrorHandler.handleErrors(ks
                            .mapValues(ProcessingResult::getError))));
        } else {
            branches = stream
                    .filter((key, value) -> value != null)
                    .split(Named.as(branchNamePrefix))
                    .branch((key, value) -> value.getError() == null, Branched.as(BRANCHING_NAME_NOMINAL))
                    .defaultBranch(Branched.withConsumer(ks -> TopologyErrorHandler.handleErrors(ks
                            .mapValues(ProcessingResult::getError))));
        }

        return branches
                .get(branchNamePrefix + BRANCHING_NAME_NOMINAL)
                .mapValues(ProcessingResult::getValue);
    }

    /**
     * Process a stream of processing errors and route it to the configured DLQ topic
     * @param errorsStream The stream of processing errors
     * @param <K> The key type
     * @param <V> The value type
     */
    private static <K, V> void handleErrors(KStream<K, ProcessingError<V>> errorsStream) {
        if (StringUtils.isBlank(KafkaStreamsExecutionContext.getDlqTopicName())) {
            log.warn("Failed to route topology error to the designated DLQ (Dead Letter Queue) topic. Please make sure to define a DLQ topic in your KafkaStreamsStarter bean configuration.");
            return;
        }
        errorsStream
                .map((key, value) -> new KeyValue<>(key == null ? "null" : key.toString(), value))
                .processValues(GenericErrorProcessor<V>::new)
                .to(KafkaStreamsExecutionContext.getDlqTopicName(), Produced.with(Serdes.String(),
                        SerdesUtils.getSerdesForValue()));
    }
}
