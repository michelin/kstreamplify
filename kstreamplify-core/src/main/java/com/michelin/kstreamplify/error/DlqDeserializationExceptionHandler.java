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

import static com.michelin.kstreamplify.property.KstreamplifyConfig.DLQ_DESERIALIZATION_HANDLER_FORWARD_REST_CLIENT_EXCEPTION;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.property.KstreamplifyConfig;
import com.michelin.kstreamplify.serde.SerdesUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;

/** The class managing deserialization exceptions. */
@Slf4j
public class DlqDeserializationExceptionHandler extends DlqExceptionHandler implements DeserializationExceptionHandler {
    private boolean handleSchemaRegistryRestException;
    private boolean continueOnUnhandledErrors;

    /** Constructor. */
    public DlqDeserializationExceptionHandler() {}

    /**
     * Handles deserialization errors by routing records to the DLQ and deciding whether to continue or fail processing
     * based on configured rules.
     */
    @Override
    public Response handleError(
            ErrorHandlerContext context, ConsumerRecord<byte[], byte[]> consumerRecord, Exception exception) {

        log.warn(
                "Exception during Deserialization, processor node: {}, taskId: {}, topic: {}, partition: {}, offset: {}",
                context.processorNodeId(),
                context.taskId(),
                context.topic(),
                context.partition(),
                context.offset(),
                exception);

        if (isDlqNotDefined()) {
            log.warn("Failed to route deserialization error to DLQ.");
            return Response.fail();
        }

        try {
            KafkaError error = buildKafkaError(context, consumerRecord, exception);
            byte[] value = serializeError(error);
            return shouldResume(exception) ? resumeWithDlqRecord(consumerRecord, value) : Response.fail();
        } catch (Exception e) {
            log.error("Cannot send deserialization exception to DLQ topic {}", deadLetterQueueTopic, e);
            return Response.fail();
        }
    }

    /** Determines if the exception should be handled by continuing processing based on known handled scenarios. */
    private boolean shouldResume(Exception exception) {
        Throwable cause = exception.getCause() != null ? exception.getCause() : exception;

        boolean isCausedByKafka = cause instanceof KafkaException;
        boolean isRestClientSchemaRegistryException = cause instanceof RestClientException;

        return isCausedByKafka
                || cause == null
                || (isRestClientSchemaRegistryException && handleSchemaRegistryRestException)
                || continueOnUnhandledErrors;
    }

    /** Builds a KafkaError enriched with record metadata and exception details for DLQ publishing. */
    private KafkaError buildKafkaError(
            ErrorHandlerContext context, ConsumerRecord<byte[], byte[]> consumerRecord, Exception exception) {

        KafkaError.Builder builder = KafkaError.newBuilder()
                .setContextMessage(
                        "An exception occurred during the stream internal deserialization. Please find more details about the exception in the cause and stack fields.")
                .setOffset(consumerRecord.offset())
                .setPartition(consumerRecord.partition())
                .setTopic(consumerRecord.topic())
                .setApplicationId(KafkaStreamsExecutionContext.getProperties().getProperty(APPLICATION_ID_CONFIG))
                .setProcessorNodeId(context.processorNodeId())
                .setTaskId(context.taskId().toString())
                .setSourceRawKey(ByteBuffer.wrap(context.sourceRawKey()))
                .setSourceRawValue(ByteBuffer.wrap(context.sourceRawValue()));

        return enrichWithException(builder, exception, consumerRecord.key(), consumerRecord.value())
                .build();
    }

    /** Serializes the KafkaError into a byte array for DLQ topic. */
    private byte[] serializeError(KafkaError error) {
        Serde<KafkaError> serde = SerdesUtils.getValueSerdes();
        return serde.serializer().serialize(deadLetterQueueTopic, error);
    }

    /** Creates a DLQ record and returns a resume response to continue processing. */
    private Response resumeWithDlqRecord(ConsumerRecord<byte[], byte[]> consumerRecord, byte[] value) {
        return Response.resume(List.of(new ProducerRecord<>(deadLetterQueueTopic, consumerRecord.key(), value)));
    }

    /** {@inheritDoc} */
    @Override
    public void configure(Map<String, ?> configs) {
        deadLetterQueueTopic = KafkaStreamsExecutionContext.getDlqTopicName();
        handleSchemaRegistryRestException = KafkaStreamsExecutionContext.isDlqFeatureEnabled(
                DLQ_DESERIALIZATION_HANDLER_FORWARD_REST_CLIENT_EXCEPTION);
        continueOnUnhandledErrors = KafkaStreamsExecutionContext.isDlqFeatureEnabled(
                KstreamplifyConfig.DLQ_DESERIALIZATION_HANDLER_CONTINUE_ON_UNHANDLED_ERRORS);
    }
}
