package io.github.michelin.spring.kafka.streams.error;

import io.github.michelin.spring.kafka.streams.avro.KafkaError;
import io.github.michelin.spring.kafka.streams.context.KafkaStreamsExecutionContext;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.util.Map;

/**
 * The class to manage DLQ production exception
 */
public class DlqProductionExceptionHandler extends DlqExceptionHandler implements ProductionExceptionHandler {
    private static final Object GUARD = new Object();

    /**
     * manage production exception
     * @param producerRecord the record to produce
     * @param productionException the exception on producing
     * @return FAIL or CONTINU
     */
    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> producerRecord, Exception productionException) {
        boolean retryable = productionException instanceof RetriableException;

        if (!retryable) {
            try {
                var builder = KafkaError.newBuilder();
                enrichWithException(builder, productionException, producerRecord.key(), producerRecord.value())
                        .setContextMessage("An exception occurred during the stream internal production")
                        .setOffset(-1)
                        .setPartition(producerRecord.partition() == null ? -1 : producerRecord.partition())
                        .setTopic(producerRecord.topic());

                producer.send(new ProducerRecord<>(KafkaStreamsExecutionContext.getDlqTopicName(), producerRecord.key(),
                        builder.build())).get();
            } catch (Exception e) {
                handleException(new String(producerRecord.key()), new String(producerRecord.value()),
                        producerRecord.topic(), e, productionException);
                return ProductionExceptionHandlerResponse.CONTINUE;
            }

            return ProductionExceptionHandlerResponse.CONTINUE;
        }

        return ProductionExceptionHandlerResponse.FAIL;
    }

    /**
     * configure the producer
     * @param configs the configuration
     */
    @Override
    public void configure(Map<String, ?> configs) {
        synchronized (GUARD) {
            if (producer == null) {
                createProducer(configs);
            }
        }
    }
}
