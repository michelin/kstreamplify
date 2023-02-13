package com.michelin.kafka.streams.starter.commons.error;

import com.michelin.kafka.streams.starter.commons.context.KafkaStreamsExecutionContext;
import com.michelin.kafka.streams.starter.commons.utils.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;

import java.util.Map;


public class TopologyErrorHandler {
    private static final String BRANCHING_NAME_NOMINAL = "branch-nominal";

    private TopologyErrorHandler() { }

    public static <K, V, V2>KStream<K,V> catchErrors(KStream<K, ProcessingResult<V, V2>> stream) {
        return catchErrors(stream, false);
    }

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

    public static <K, V> void handleErrors(KStream<K, ProcessingError<V>> errorsStream) {
        errorsStream
                .map((key, value) -> new KeyValue<>(key == null ? "null" : key.toString(), value))
                .processValues(GenericErrorProcessor<V>::new)
                .to(KafkaStreamsExecutionContext.getDlqTopicName(), Produced.with(Serdes.String(),
                        SerdesUtils.getSerdesForValue()));
    }
}
