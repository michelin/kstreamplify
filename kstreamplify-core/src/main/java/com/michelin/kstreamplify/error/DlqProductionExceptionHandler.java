/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.kstreamplify.error;

import static com.michelin.kstreamplify.property.KstreamplifyConfig.DLQ_PRODUCTION_HANDLER_CONTINUE_ON_SERIALIZATION_EXCEPTION;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.serde.SerdesUtils;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

/**
 * Production exception handler that routes failed records to a Dead Letter Queue (DLQ) topic. Handles both general
 * production errors and serialization errors by wrapping failed records in a {@link KafkaError} and sending them to the
 * configured DLQ topic.
 */
@Slf4j
public class DlqProductionExceptionHandler extends DlqExceptionHandler implements ProductionExceptionHandler {
    private boolean continueOnSerializationException;

    /** Constructor. */
    public DlqProductionExceptionHandler() {}

    /**
     * Handles production exceptions by routing failed records to the DLQ topic. Returns FAIL if no DLQ is configured,
     * RETRY for retriable exceptions, or RESUME after sending to DLQ.
     *
     * @param context the error handler context with metadata about the error
     * @param producerRecord the producer record that failed to be produced
     * @param exception the exception that occurred during production
     * @return a {@link Response} indicating how to proceed (FAIL, RETRY, or RESUME)
     */
    @Override
    public Response handleError(
            ErrorHandlerContext context, ProducerRecord<byte[], byte[]> producerRecord, Exception exception) {
        log.warn(
                "Exception during message Production, processor node: {}, taskId: {}, source topic: {}, source partition: {}, source offset: {}",
                context.processorNodeId(),
                context.taskId(),
                context.topic(),
                context.partition(),
                context.offset(),
                exception);

        if (isDlqNotDefined()) {
            log.warn("Failed to route production error to DLQ. Define a DLQ topic in configuration.");
            return Response.fail();
        }

        if (exception instanceof RetriableException) {
            return Response.retry();
        }

        try {
            KafkaError.Builder builder = KafkaError.newBuilder()
                    .setContextMessage(
                            "An exception occurred during the stream internal production. Please find more details about the exception in the cause and stack fields.")
                    .setOffset(context.offset())
                    .setPartition(context.partition())
                    .setTopic(producerRecord.topic())
                    .setApplicationId(
                            KafkaStreamsExecutionContext.getProperties().getProperty(APPLICATION_ID_CONFIG))
                    .setProcessorNodeId(context.processorNodeId())
                    .setTaskId(context.taskId().toString());

            KafkaError error = enrichWithException(builder, exception, producerRecord.key(), producerRecord.value())
                    .build();

            Serde<KafkaError> serde = SerdesUtils.getValueSerdes();
            byte[] value = serde.serializer().serialize(deadLetterQueueTopic, error);

            return Response.resume(List.of(new ProducerRecord<>(deadLetterQueueTopic, producerRecord.key(), value)));
        } catch (Exception e) {
            log.error(
                    "Cannot send production exception to DLQ topic {}",
                    KafkaStreamsExecutionContext.getDlqTopicName(),
                    e);
            return Response.resume();
        }
    }

    /**
     * Handles serialization exceptions by routing failed records with raw bytes to the DLQ topic. Behavior is
     * controlled by the {@code continueOnSerializationException} flag. Returns FAIL if the flag is disabled or no DLQ
     * is configured, otherwise RESUME after sending to DLQ.
     *
     * @param context the error handler context with access to source raw bytes
     * @param record the producer record that failed serialization
     * @param exception the serialization exception that occurred
     * @param origin the origin of the serialization exception (KEY or VALUE)
     * @return a {@link Response} indicating how to proceed (FAIL or RESUME)
     */
    @Override
    public Response handleSerializationError(
            ErrorHandlerContext context,
            ProducerRecord record,
            Exception exception,
            SerializationExceptionOrigin origin) {
        log.warn(
                "Serialization exception during message Production, origin: {}, processor node: {}, taskId: {}, source topic: {}, source partition: {}, source offset: {}",
                origin,
                context.processorNodeId(),
                context.taskId(),
                context.topic(),
                context.partition(),
                context.offset(),
                exception);

        if (!continueOnSerializationException) {
            return Response.fail();
        }

        if (isDlqNotDefined()) {
            log.warn("Failed to route serialization error to DLQ. Define a DLQ topic in configuration.");
            return Response.fail();
        }

        try {
            KafkaError.Builder builder = KafkaError.newBuilder()
                    .setContextMessage("A serialization exception occurred during the stream internal production. "
                            + "Origin: " + origin + ". "
                            + "Please find more details about the exception in the cause and stack fields.")
                    .setOffset(context.offset())
                    .setPartition(context.partition())
                    .setTopic(record.topic())
                    .setApplicationId(
                            KafkaStreamsExecutionContext.getProperties().getProperty(APPLICATION_ID_CONFIG))
                    .setProcessorNodeId(context.processorNodeId())
                    .setTaskId(context.taskId().toString());

            KafkaError error = enrichWithException(builder, exception, context.sourceRawKey(), context.sourceRawValue())
                    .build();

            Serde<KafkaError> serde = SerdesUtils.getValueSerdes();
            byte[] value = serde.serializer().serialize(deadLetterQueueTopic, error);

            return Response.resume(List.of(new ProducerRecord<>(deadLetterQueueTopic, context.sourceRawKey(), value)));
        } catch (Exception e) {
            log.error(
                    "Cannot send serialization exception to DLQ topic {}",
                    KafkaStreamsExecutionContext.getDlqTopicName(),
                    e);
            return Response.resume();
        }
    }

    /**
     * Configures the handler with DLQ topic name and serialization exception handling flag.
     *
     * @param configs the configuration map provided by Kafka Streams
     */
    @Override
    public void configure(Map<String, ?> configs) {
        deadLetterQueueTopic = KafkaStreamsExecutionContext.getDlqTopicName();
        continueOnSerializationException = KafkaStreamsExecutionContext.isDlqFeatureEnabled(
                DLQ_PRODUCTION_HANDLER_CONTINUE_ON_SERIALIZATION_EXCEPTION);
    }
}
