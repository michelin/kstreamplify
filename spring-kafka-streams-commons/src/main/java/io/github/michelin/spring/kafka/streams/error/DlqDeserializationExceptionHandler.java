package io.github.michelin.spring.kafka.streams.error;

import io.github.michelin.spring.kafka.streams.avro.KafkaError;
import io.github.michelin.spring.kafka.streams.context.KafkaStreamsExecutionContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;

/**
 * The class to manage deserialization exception
 */
public class DlqDeserializationExceptionHandler extends DlqExceptionHandler implements DeserializationExceptionHandler {
    private static final Object GUARD = new Object();

    /**
     * manage deserialization exception
     * @param processorContext the processor context
     * @param consumerRecord the record to deserialize
     * @param consumptionException the exception for the deserialization
     * @return FAIL or CONTINUE
     */
    @Override
    public DeserializationHandlerResponse handle(ProcessorContext processorContext, ConsumerRecord<byte[], byte[]> consumerRecord, Exception consumptionException) {
        try {
            var builder = KafkaError.newBuilder();
            enrichWithException(builder, consumptionException, consumerRecord.key(), consumerRecord.value())
                    .setContextMessage("An exception occurred during the stream internal deserialization")
                    .setOffset(processorContext.offset())
                    .setPartition(processorContext.partition())
                    .setTopic(processorContext.topic());

            producer.send(new ProducerRecord<>(KafkaStreamsExecutionContext.getDlqTopicName(), consumerRecord.key(), builder.build())).get();
        } catch (Exception e) {
            handleException(new String(consumerRecord.key()), new String(consumerRecord.value()),
                    consumerRecord.topic(), e, consumptionException);
            return DeserializationHandlerResponse.FAIL;
        }

        return DeserializationHandlerResponse.CONTINUE;
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
