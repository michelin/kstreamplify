package com.michelin.kafka.streams.starter.commons.error;

import com.michelin.kafka.streams.starter.commons.context.KafkaStreamsExecutionContext;
import com.michelin.kafka.streams.starter.commons.utils.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;

import java.util.Map;


public class ErrorHandler {
    private static final String BRANCHING_NAME_NOMINAL = "branch-nominal";

    private ErrorHandler() { }

    public static <K, V, V2>KStream<K,V> catchErrors(KStream<K, ProcessingResult<V, V2>> stream) {
        return catchErrors(stream, false);
    }

    public static <K, V, V2> KStream<K,V> catchErrors(KStream<K, ProcessingResult<V, V2>> stream, boolean allowTombstone) {
        Map<String, KStream<K,ProcessingResult<V, V2>>> branches;

        String branchNamePrefix = stream.toString().split("@")[1];
        if (!allowTombstone) {
            branches = stream
                    .filter((k,v) -> v !=  null)
                    .filterNot((k,v) -> v.getValue() == null && v.getError() == null)
                    .split(Named.as(branchNamePrefix))
                    .branch((key, value) -> value.isValid(), Branched.as(BRANCHING_NAME_NOMINAL))
                    .defaultBranch(Branched.withConsumer(ks -> ErrorHandler.handleErrors(ks
                            .mapValues(ProcessingResult::getError))));
        } else {
            branches = stream
                    .filter((k,v) -> v !=  null)
                    .split(Named.as(branchNamePrefix))
                    .branch((key, value) -> value.getError() == null, Branched.as(BRANCHING_NAME_NOMINAL))
                    .defaultBranch(Branched.withConsumer(ks -> ErrorHandler.handleErrors(ks
                            .mapValues(ProcessingResult::getError))));
        }

        return branches
                .get(branchNamePrefix + BRANCHING_NAME_NOMINAL)
                .mapValues(ProcessingResult::getValue);
    }

    public static <K, V> void handleErrors(KStream<K, ProcessingError<V>> inputStream) {
        inputStream
                .map((k,v) -> new KeyValue<>(k == null ? "null" : k.toString(), v))
                .transformValues(GenericErrorTransformer<V>::new)
                .to(KafkaStreamsExecutionContext.getDlqTopicName(), Produced.with(Serdes.String(), SerdesUtils.getSerdesForValue()));
    }
}
