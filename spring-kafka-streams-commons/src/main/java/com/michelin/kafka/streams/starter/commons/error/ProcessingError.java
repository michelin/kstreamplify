package com.michelin.kafka.streams.starter.commons.error;

import com.michelin.kafka.streams.starter.commons.converter.AvroToJsonConverter;
import lombok.Getter;
import org.apache.avro.generic.GenericRecord;

@Getter
public class ProcessingError<V> {
    private final Exception exception;
    private final String message;
    private final String contextMessage;

    public ProcessingError(Exception e, String contextMessage, V messageValue) {
        this.exception = e;
        this.contextMessage = contextMessage;

        if (messageValue instanceof GenericRecord) {
            this.message = AvroToJsonConverter.convertRecord((GenericRecord) messageValue);
        } else {
            this.message = String.valueOf(messageValue);
        }
    }

    public ProcessingError(Exception exception, V messageValue) {
        this(exception,(exception != null ? exception.getMessage() : null), messageValue);
    }
}
