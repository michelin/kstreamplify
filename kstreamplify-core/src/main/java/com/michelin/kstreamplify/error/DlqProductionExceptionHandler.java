package com.michelin.kstreamplify.error;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

/**
 * The class managing DLQ production exceptions.
 */
@Slf4j
public class DlqProductionExceptionHandler extends DlqExceptionHandler
    implements ProductionExceptionHandler {
    private static final Object GUARD = new Object();

    /**
     * Manage production exceptions.
     *
     * @param producerRecord      the record to produce
     * @param productionException the exception on producing
     * @return FAIL or CONTINUE
     */
    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> producerRecord,
                                                     Exception productionException) {
        if (StringUtils.isBlank(KafkaStreamsExecutionContext.getDlqTopicName())) {
            log.warn(
                "Failed to route production error to the designated "
                    + "DLQ (Dead Letter Queue) topic. "
                    + "Please make sure to define a DLQ topic in your "
                    + "KafkaStreamsStarter bean configuration.");
            return ProductionExceptionHandlerResponse.FAIL;
        }

        boolean retryable = productionException instanceof RetriableException;

        if (!retryable) {
            try {
                var builder = KafkaError.newBuilder();
                enrichWithException(builder, productionException, producerRecord.key(),
                    producerRecord.value())
                    .setContextMessage(
                        "An exception occurred during the stream internal production")
                    .setOffset(-1)
                    .setPartition(
                        producerRecord.partition() == null ? -1 : producerRecord.partition())
                    .setTopic(producerRecord.topic());

                producer.send(new ProducerRecord<>(KafkaStreamsExecutionContext.getDlqTopicName(),
                    producerRecord.key(), builder.build())).get();
            } catch (InterruptedException ie) {
                log.error(
                    "Interruption while sending the production exception {} for key {}, "
                        + "value {} and topic {} to DLQ topic {}",
                    productionException,
                    producerRecord.key(), producerRecord.value(), producerRecord.topic(),
                    KafkaStreamsExecutionContext.getDlqTopicName(), ie);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error(
                    "Cannot send the production exception {} for key {}, value {}"
                        + " and topic {} to DLQ topic {}",
                    productionException,
                    producerRecord.key(), producerRecord.value(), producerRecord.topic(),
                    KafkaStreamsExecutionContext.getDlqTopicName(), e);
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
