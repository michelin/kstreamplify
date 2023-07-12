package io.github.michelin.kstreamplify.error;

import com.michelin.kstreamplify.avro.KafkaError;
import io.github.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;

/**
 * The class managing deserialization exceptions
 */
@Slf4j
public class DlqDeserializationExceptionHandler extends DlqExceptionHandler implements DeserializationExceptionHandler {
    private static final Object GUARD = new Object();

    /**
     * Manage deserialization exceptions
     *
     * @param processorContext     the processor context
     * @param consumerRecord       the record to deserialize
     * @param consumptionException the exception for the deserialization
     * @return FAIL or CONTINUE
     */
    @Override
    public DeserializationHandlerResponse handle(ProcessorContext processorContext, ConsumerRecord<byte[], byte[]> consumerRecord, Exception consumptionException) {
        if (StringUtils.isBlank(KafkaStreamsExecutionContext.getDlqTopicName())) {
            log.warn("Failed to route deserialization error to the designated DLQ (Dead Letter Queue) topic. Please make sure to define a DLQ topic in your KafkaStreamsStarter bean configuration.");
            return DeserializationHandlerResponse.FAIL;
        }

        try {
            var builder = KafkaError.newBuilder();
            enrichWithException(builder, consumptionException, consumerRecord.key(), consumerRecord.value())
                    .setContextMessage("An exception occurred during the stream internal deserialization")
                    .setOffset(processorContext.offset())
                    .setPartition(processorContext.partition())
                    .setTopic(processorContext.topic());

            producer.send(new ProducerRecord<>(KafkaStreamsExecutionContext.getDlqTopicName(), consumerRecord.key(), builder.build())).get();
        } catch (Exception e) {
            log.error("Cannot send the deserialization exception {} for key {}, value {} and topic {} to DLQ topic {}", consumptionException,
                    consumerRecord.key(), consumerRecord.value(), consumerRecord.topic(), KafkaStreamsExecutionContext.getDlqTopicName(), e);
            return DeserializationHandlerResponse.FAIL;
        }

        return DeserializationHandlerResponse.CONTINUE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(Map<String, ?> configs) {
        synchronized (GUARD) {
            if (producer == null) {
                instantiateProducer(DlqDeserializationExceptionHandler.class.getName(), configs);
            }
        }
    }
}
