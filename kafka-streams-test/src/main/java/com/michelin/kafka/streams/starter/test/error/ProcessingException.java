package com.michelin.kafka.streams.starter.test.error;

import com.michelin.kafka.streams.starter.test.converter.AvroToJsonConverter;
import org.apache.avro.generic.GenericRecord;

/**
 * Hold an exception with the current message
 */
public class ProcessingException<V> {

    private final Exception e;
    private final String message;
    private final String contextMessage;

    public ProcessingException(Exception e,String contextMessage, V messageValue) {
        this.e = e;
        this.contextMessage = contextMessage;

        if(messageValue instanceof GenericRecord) {
            this.message = AvroToJsonConverter.convertRecord((GenericRecord) messageValue);
        } else {
            this.message = String.valueOf(messageValue);
        }
    }

    public ProcessingException(Exception e, V messageValue) {
        this(e,(e != null ? e.getMessage():null),messageValue);
    }

    /**
     * @return the exeception encountered during processing
     */
    public Exception getException() {
        return e;
    }

    /**
     * @return the current message when processing exception was encountered
     */
    public String getMessage() {
        return message;
    }

    /**
     * @return the current context message when processing exception was encountered or exception message if not set
     */
    public String getContextMessage() {
        return contextMessage;
    }
}
