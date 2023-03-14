package com.michelin.kafka.streams.starter.commons.error;

import com.michelin.kafka.streams.starter.avro.GenericError;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;

import java.io.PrintWriter;
import java.io.StringWriter;

public class GenericErrorProcessor<V> implements FixedKeyProcessor<String, ProcessingError<V>, GenericError> {
    private FixedKeyProcessorContext<String, GenericError> context;

    @Override
    public void init(FixedKeyProcessorContext<String, GenericError> context) {
        this.context = context;
    }

    @Override
    public void process(FixedKeyRecord<String, ProcessingError<V>> fixedKeyRecord) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        fixedKeyRecord.value().getException().printStackTrace(pw);

        RecordMetadata recordMetadata = context.recordMetadata().orElse(null);

        GenericError error = GenericError.newBuilder()
                .setCause(fixedKeyRecord.value().getException().getMessage())
                .setContextMessage(fixedKeyRecord.value().getContextMessage())
                .setOffset(recordMetadata != null ? recordMetadata.offset() : -1)
                .setPartition(recordMetadata != null ? recordMetadata.partition() : -1)
                .setStack(sw.toString())
                .setTopic(recordMetadata != null && recordMetadata.topic() != null ? recordMetadata.topic() : "Outside topic context")
                .setValue(fixedKeyRecord.value().getMessage())
                .build();

        context.forward(fixedKeyRecord.withValue(error));
    }

    @Override
    public void close() {
        // may close resource opened in init
    }
}
