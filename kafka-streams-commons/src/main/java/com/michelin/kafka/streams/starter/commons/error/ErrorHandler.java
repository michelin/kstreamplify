package com.michelin.kafka.streams.starter.commons.error;

import com.michelin.kafka.streams.starter.avro.GenericError;
import com.michelin.kafka.streams.starter.initializer.KafkaStreamsExecutionContext;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;

import java.util.Map;


/**
 * 
 * 
 * Catch and redirect all exception into the context DLQ
 */

/**
 * Helper for stream error management
 */
public class ErrorHandler {

    private static final String BRANCHING_NAME_ERROR = "branch-error";
    private static final String BRANCHING_NAME_NOMINAL = "branch-nominal";

    private static SpecificAvroSerde<GenericError> getErrorSerde() {
        SpecificAvroSerde<GenericError> serde = new SpecificAvroSerde<>();
        serde.configure(KafkaStreamsExecutionContext.getSerdesConfig(), false);
        return serde;
    }

    /**
     * 
     * @Deprecated Must be replaced by #processResult method
     * 
     * @param inputStream the stream which need some error handling
     * @param <K> stream key
     * @param <V> stream initial value ( without exception)
     * @return
     */
    public static <K, V>KStream<K,V> catchError(KStream<K, ?> inputStream) {

        String branchNamePrefix = inputStream.toString().split("@")[1];

        Map<String, ? extends KStream<K, ?>> branches = inputStream.split(Named.as(branchNamePrefix))
                .branch((key, value) -> value instanceof ProcessingException, Branched.as(BRANCHING_NAME_ERROR))
                .branch((key, value) -> true, Branched.as(BRANCHING_NAME_NOMINAL))
                .noDefaultBranch();

        KStream<K, ProcessingException> errorOutput = branches.get(branchNamePrefix+BRANCHING_NAME_ERROR).mapValues(e -> (ProcessingException) e);
        KStream<K, V> nominalOutput = branches.get(branchNamePrefix+BRANCHING_NAME_NOMINAL).mapValues(e -> (V) e);

        errorOutput
                .map( (k,v) -> new KeyValue<>(k == null ? "null": k.toString(), v))
                .transformValues(GenericErrorTransformer::new)
                .to(KafkaStreamsExecutionContext.getDlqTopicName(), Produced.with(Serdes.String(), getErrorSerde()));
                        
        return nominalOutput;
    }

    /**
     * @param inputStream a stream of processing result
     * @param <K> stream key
     * @param <V> stream initial value (without exception)
     * @return
     */
    public static <K, V, EV>KStream<K,V> processResult(KStream<K, ProcessingResult<V, EV>> inputStream) {
        return processResult(inputStream, false);
    }

    /**
     * This version 
     * 
     * @param inputStream a stream of processing result
     * @param <K> stream key
     * @param <V> stream initial value (without exception)
     * @return
     */
    public static <K, V, EV>KStream<K,V> processResult(KStream<K, ProcessingResult<V, EV>> inputStream, boolean allowTombstone) {
        Map<String, KStream<K,ProcessingResult<V, EV>>> branches;

        String branchNamePrefix = inputStream.toString().split("@")[1];

        if(!allowTombstone) {
            branches = inputStream
                    .filter((k,v) -> v !=  null)
                    .filterNot((k,v) -> v.getValue() == null && v.getException() == null)
                    .split(Named.as(branchNamePrefix))
                    .branch((key, value) -> value.isValid(), Branched.as(BRANCHING_NAME_NOMINAL))
                    .branch((key, value) -> true, Branched.as(BRANCHING_NAME_ERROR))
                    .noDefaultBranch();
        } else {
            branches = inputStream
                    .filter((k,v) -> v !=  null)
                    .split(Named.as(branchNamePrefix))
                    .branch((key, value) -> value.getException() == null, Branched.as(BRANCHING_NAME_NOMINAL))
                    .branch((key, value) -> true, Branched.as(BRANCHING_NAME_ERROR))
                    .noDefaultBranch();;
        }


        KStream<K, ProcessingException> errorOutput = branches.get(branchNamePrefix+BRANCHING_NAME_ERROR).mapValues(ProcessingResult::getException);
        KStream<K, V> nominalOutput = branches.get(branchNamePrefix+BRANCHING_NAME_NOMINAL).mapValues(ProcessingResult::getValue);

        errorOutput
                .map( (k,v) -> new KeyValue<>(k == null ? "null": k.toString(), v))
                .transformValues(GenericErrorTransformer::new)
                .to(KafkaStreamsExecutionContext.getDlqTopicName(), Produced.with(Serdes.String(), getErrorSerde()));

        return nominalOutput;
    }

    /**
     * @param inputStream a stream emitting processing exceptions
     */
    public static <K, V>void handleError(KStream<K, ProcessingException<V>> inputStream) {
        
        inputStream
                .map( (k,v) -> new KeyValue<>(k == null ? "null": k.toString(), (ProcessingException)v))
                .transformValues(GenericErrorTransformer::new)
                .to(KafkaStreamsExecutionContext.getDlqTopicName(), Produced.with(Serdes.String(), getErrorSerde()));
    }
}
