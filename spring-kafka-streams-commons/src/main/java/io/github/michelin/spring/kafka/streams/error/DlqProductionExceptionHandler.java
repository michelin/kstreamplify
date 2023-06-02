package io.github.michelin.spring.kafka.streams.error;

import io.github.michelin.spring.kafka.streams.avro.KafkaError;
import io.github.michelin.spring.kafka.streams.context.KafkaStreamsExecutionContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.util.Map;

/**
 * The class managing DLQ production exceptions
 */
@Slf4j
public class DlqProductionExceptionHandler extends DlqExceptionHandler implements ProductionExceptionHandler {
    private static final Object GUARD = new Object();

    /**
     * Manage production exceptions
     * @param producerRecord the record to produce
     * @param productionException the exception on producing
     * @return FAIL or CONTINUE
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

                producer.send(new ProducerRecord<>(KafkaStreamsExecutionContext.getDlqTopicName(), producerRecord.key(), builder.build())).get();
            } catch (Exception e) {
                log.error("Cannot send the production exception {} for key {}, value {} and topic {} to DLQ topic {}", productionException,
                        producerRecord.key(), producerRecord.value(), producerRecord.topic(), KafkaStreamsExecutionContext.getDlqTopicName(), e);
                return ProductionExceptionHandlerResponse.CONTINUE;
            }

            return ProductionExceptionHandlerResponse.CONTINUE;
        }

        return ProductionExceptionHandlerResponse.FAIL;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(Map<String, ?> configs) {
        synchronized (GUARD) {
            if (producer == null) {
                instantiateProducer(DlqProductionExceptionHandler.class.getName(), configs);
            }
        }
    }
}
