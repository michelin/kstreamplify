package io.github.michelin.spring.kafka.streams.error;

import io.github.michelin.spring.kafka.streams.converter.AvroToJsonConverter;
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
    private final String message;

    /**
     * A context message defined when the error is caught
     */
    private final String contextMessage;

    /**
     * Constructor
     * @param e The exception
     * @param contextMessage The context message
     * @param messageValue The failed Kafka record
     */
    public ProcessingError(Exception e, String contextMessage, V messageValue) {
        this.exception = e;
        this.contextMessage = contextMessage;

        if (messageValue instanceof GenericRecord) {
            this.message = AvroToJsonConverter.convertRecord((GenericRecord) messageValue);
        } else {
            this.message = String.valueOf(messageValue);
        }
    }

    /**
     * Constructor
     * @param exception The exception
     * @param messageValue The failed Kafka record
     */
    public ProcessingError(Exception exception, V messageValue) {
        this(exception,(exception != null ? exception.getMessage() : null), messageValue);
    }
}
