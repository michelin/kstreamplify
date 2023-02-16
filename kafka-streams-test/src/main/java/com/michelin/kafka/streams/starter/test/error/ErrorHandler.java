package com.michelin.kafka.streams.starter.test.error;

import com.michelin.kafka.streams.starter.avro.GenericError;
import com.michelin.kafka.streams.starter.init.KafkaStreamsExecutionContext;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * 
 * Catch and redirect all exception into the context DLQ
 */

/**
 * Helper for stream error management
 */
public class ErrorHandler {

    final static Logger logger = LoggerFactory.getLogger(ErrorHandler.class.getName());



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

        var branches = inputStream.branch(
            (key, value) -> value instanceof ProcessingException, /* Any error occured  */
            (key, value) -> true /* nominal case  */
        );
        var errorOutput = branches[0].mapValues( e -> (ProcessingException) e);
        var nominalOutput = branches[1].mapValues( e -> (V) e);

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


        KStream<K,ProcessingResult<V, EV>>[] branches = null;
        
        if(!allowTombstone) {
            branches = inputStream
                    .filter((k,v) -> v !=  null) // Remove null value from stream
                    .filterNot((k,v) -> v.getValue() == null && v.getException() == null) // Remove nprocess result will null content
                    .branch(
                            (key, value) ->
                                    value.isValid(), /* standard output */
                            (key, value) -> true /* error output  */
                    );
        } else {
            branches = inputStream
                    .filter((k,v) -> v !=  null) // Remove null value from stream
                    .branch(
                            (key, value) ->
                                    value.getException() == null, /* standard output */
                            (key, value) -> true /* error output  */
                    );
        }

        var errorOutput = branches[1].mapValues(ProcessingResult::getException);
        var nominalOutput = branches[0].mapValues(ProcessingResult::getValue);

        errorOutput
                .map( (k,v) -> new KeyValue<>(k == null ? "null": k.toString(), (ProcessingException)v))
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
    
    
    /**
     * @param applicationId the application common name
     * @param e the exception threw by the kafka
     */
    public static void closeAndLogStreams(String applicationId, Throwable e ){
        logger.error("Severe irrecoverable error occurred in " + applicationId + " stream", e);
        logger.error("Kafka Stream is currently shuting down");
    }
}
