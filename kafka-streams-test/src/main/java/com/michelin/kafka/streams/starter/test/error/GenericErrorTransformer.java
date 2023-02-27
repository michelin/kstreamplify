package com.michelin.kafka.streams.starter.test.error;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import com.michelin.kafka.streams.starter.avro.GenericError;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Transform an exception in the stream to message for DLQ
 */
public class GenericErrorTransformer implements ValueTransformer<ProcessingException, GenericError> {

    private ProcessorContext localContext;

    @Override
    public void init(ProcessorContext processorContext) {
        localContext = processorContext;
    }

    @Override
    public GenericError transform(ProcessingException e) {

        var offset = localContext.offset();
        var partition = localContext.partition();
        var topic = localContext.topic();
        var message = e.getMessage();
        var exception = e.getException();

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.getException().printStackTrace(pw);

        return GenericError.newBuilder()
                .setCause(exception.getMessage())
                .setContextMessage(e.getContextMessage())
                .setOffset(offset)
                .setPartition(partition)
                .setStack(sw.toString())
                .setTopic(topic == null ? "Outside topic context" : topic)
                .setValue(message)
                .build();
    }

    @Override
    public void close() {
        // may close resource opened in init
    }
}
