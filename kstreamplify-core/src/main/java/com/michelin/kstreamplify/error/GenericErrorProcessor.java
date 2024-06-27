package com.michelin.kstreamplify.error;

import com.michelin.kstreamplify.avro.KafkaError;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;

/**
 * Generic error processor.
 *
 * @param <V> The type of the failed record
 */
class GenericErrorProcessor<V> extends ContextualFixedKeyProcessor<String, ProcessingError<V>, KafkaError> {
    /**
     * Process the error.
     *
     * @param fixedKeyRecord the record to process an error
     */
    @Override
    public void process(FixedKeyRecord<String, ProcessingError<V>> fixedKeyRecord) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        fixedKeyRecord.value().getException().printStackTrace(pw);

        RecordMetadata recordMetadata = context().recordMetadata().orElse(null);

        KafkaError error = KafkaError.newBuilder()
            .setCause(fixedKeyRecord.value().getException().getMessage())
            .setContextMessage(fixedKeyRecord.value().getContextMessage())
            .setOffset(recordMetadata != null ? recordMetadata.offset() : -1)
            .setPartition(recordMetadata != null ? recordMetadata.partition() : -1)
            .setStack(sw.toString())
            .setTopic(
                recordMetadata != null && recordMetadata.topic() != null ? recordMetadata.topic() :
                    "Outside topic context")
            .setValue(fixedKeyRecord.value().getKafkaRecord())
            .build();

        context().forward(fixedKeyRecord.withValue(error));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        // may close resource opened in init
    }
}
