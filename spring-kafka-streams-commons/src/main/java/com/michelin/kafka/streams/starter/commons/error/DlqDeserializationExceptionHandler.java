package com.michelin.kafka.streams.starter.commons.error;

import com.michelin.kafka.streams.starter.avro.KafkaError;
import com.michelin.kafka.streams.starter.commons.context.KafkaStreamsExecutionContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;

public class DlqDeserializationExceptionHandler extends DlqExceptionHandler implements DeserializationExceptionHandler {
    private static final Object GUARD = new Object();

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

    @Override
    public void configure(Map<String, ?> configs) {
        synchronized (GUARD) {
            if (producer == null) {
                createProducer(configs);
            }
        }
    }
}
