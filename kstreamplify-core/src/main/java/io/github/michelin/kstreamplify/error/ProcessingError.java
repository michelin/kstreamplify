package io.github.michelin.kstreamplify.error;

import io.github.michelin.kstreamplify.converter.AvroToJsonConverter;
import lombok.Getter;
import org.apache.avro.generic.GenericRecord;

/**
 * The processing error class
 * @param <V> The type of the failed record
 */
@Getter
public class ProcessingError<V> {
    /**
     * The exception that occurred
     */
    private final Exception exception;

    /**
     * The failed Kafka record
     */
    private final String kafkaRecord;

    /**
     * A context message defined when the error is caught
     */
    private final String contextMessage;

    /**
     * Constructor
     * @param e The exception
     * @param contextMessage The context message
     * @param kafkaRecord The failed Kafka record
     */
    public ProcessingError(Exception e, String contextMessage, V kafkaRecord) {
        this.exception = e;
        this.contextMessage = contextMessage;

        if (kafkaRecord instanceof GenericRecord genericRecord) {
            this.kafkaRecord = AvroToJsonConverter.convertRecord(genericRecord);
        } else {
            this.kafkaRecord = String.valueOf(kafkaRecord);
        }
    }

    /**
     * Constructor
     * @param exception The exception
     * @param kafkaRecord The failed Kafka record
     */
    public ProcessingError(Exception exception, V kafkaRecord) {
        this(exception, "No context message", kafkaRecord);
    }
}
