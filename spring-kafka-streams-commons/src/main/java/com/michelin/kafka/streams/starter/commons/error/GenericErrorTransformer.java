package com.michelin.kafka.streams.starter.commons.error;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import com.michelin.kafka.streams.starter.avro.GenericError;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Transform an exception in the stream to message for DLQ
 */
public class GenericErrorTransformer<V> implements ValueTransformer<ProcessingError<V>, GenericError> {

    private ProcessorContext processorContext;

    @Override
    public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;
    }

    @Override
    public GenericError transform(ProcessingError e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.getException().printStackTrace(pw);

        return GenericError.newBuilder()
                .setCause(e.getException().getMessage())
                .setContextMessage(e.getContextMessage())
                .setOffset(processorContext.offset())
                .setPartition(processorContext.partition())
                .setStack(sw.toString())
                .setTopic(processorContext.topic() == null ? "Outside topic context" : processorContext.topic())
                .setValue(e.getMessage())
                .build();
    }

    @Override
    public void close() {
        // may close resource opened in init
    }
}
