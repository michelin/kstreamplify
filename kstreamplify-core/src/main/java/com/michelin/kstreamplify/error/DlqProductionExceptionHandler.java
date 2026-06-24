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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The class managing production exceptions. */
public class DlqProductionExceptionHandler extends DlqExceptionHandler implements ProductionExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(DlqProductionExceptionHandler.class);

    /** Constructor. */
    public DlqProductionExceptionHandler() {}

    /**
     * Handles production exceptions by routing the record to the DLQ topic.
     *
     * @param context The error handler context
     * @param producerRecord The record that failed production
     * @param exception The exception that occurred
     * @return A {@link Response} indicating how to proceed
     */
    @Override
    public Response handleError(
            ErrorHandlerContext context, ProducerRecord<byte[], byte[]> producerRecord, Exception exception) {
        log.warn(
                "Exception during production, processor node: {}, taskId: {}, topic: {}, partition: {}, offset: {}",
                context.processorNodeId(),
                context.taskId(),
                context.topic(),
                context.partition(),
                context.offset(),
                exception);

        if (exception instanceof RetriableException) {
            return Response.retry();
        }

        if (isDlqNotDefined()) {
            log.warn("Failed to route production error to DLQ. Define a DLQ topic in configuration.");
            return Response.fail();
        }

        try {
            KafkaError error = buildKafkaError(
                    context, producerRecord.topic(), producerRecord.key(), producerRecord.value(), exception, null);

            Serde<KafkaError> serde = SerdesUtils.getValueSerdes();
            byte[] value = serde.serializer().serialize(KafkaStreamsExecutionContext.getDlqTopicName(), error);

            return Response.resume(List.of(
                    new ProducerRecord<>(KafkaStreamsExecutionContext.getDlqTopicName(), producerRecord.key(), value)));
        } catch (Exception e) {
            log.error(
                    "Cannot send production exception to DLQ topic {}",
                    KafkaStreamsExecutionContext.getDlqTopicName(),
                    e);
            return Response.resume();
        }
    }

    /**
     * Handles serialization exceptions by routing the record to the DLQ topic.
     *
     * @param context The error handler context
     * @param record The record that failed serialization
     * @param exception The exception that occurred
     * @param origin The origin of the serialization exception
     * @return A {@link Response} indicating how to proceed
     */
    @Override
    public Response handleSerializationError(
            ErrorHandlerContext context,
            ProducerRecord record,
            Exception exception,
            SerializationExceptionOrigin origin) {
        log.warn(
                "Exception during serialization, origin: {}, processor node: {}, taskId: {}, topic: {}, partition: {}, offset: {}",
                origin,
                context.processorNodeId(),
                context.taskId(),
                context.topic(),
                context.partition(),
                context.offset(),
                exception);

        if (isDlqNotDefined()) {
            log.warn("Failed to route serialization error to DLQ. Define a DLQ topic in configuration.");
            return Response.fail();
        }

        boolean continueOnSerializationException = KafkaStreamsExecutionContext.isDlqFeatureEnabled(
                DLQ_PRODUCTION_HANDLER_CONTINUE_ON_SERIALIZATION_EXCEPTION);
        if (!continueOnSerializationException) {
            return Response.fail();
        }

        try {
            KafkaError error = buildKafkaError(
                    context, record.topic(), context.sourceRawKey(), context.sourceRawValue(), exception, origin);
            Serde<KafkaError> serde = SerdesUtils.getValueSerdes();
            byte[] value = serde.serializer().serialize(KafkaStreamsExecutionContext.getDlqTopicName(), error);
            return Response.resume(List.of(new ProducerRecord<>(
                    KafkaStreamsExecutionContext.getDlqTopicName(), context.sourceRawKey(), value)));
        } catch (Exception e) {
            log.error(
                    "Cannot send serialization exception to DLQ topic {}",
                    KafkaStreamsExecutionContext.getDlqTopicName(),
                    e);
            return Response.fail();
        }
    }

    /**
     * Builds a {@link KafkaError} from the record metadata and exception details.
     *
     * @param context The error handler context
     * @param topic The topic of the record that failed
     * @param key The raw key of the record that failed
     * @param value The raw value of the record that failed
     * @param exception The exception that occurred
     * @param origin The origin of the serialization exception, or {@code null} for a production exception
     * @return The built {@link KafkaError}
     */
    private KafkaError buildKafkaError(
            ErrorHandlerContext context,
            String topic,
            byte[] key,
            byte[] value,
            Exception exception,
            SerializationExceptionOrigin origin) {

        String contextMessage = origin == null
                ? "An exception occurred during the stream internal production. "
                        + "Please find more details about the exception in the cause and stack fields."
                : "A serialization exception occurred during the stream internal production. "
                        + "Origin: " + origin + ". "
                        + "Please find more details about the exception in the cause and stack fields.";

        KafkaError.Builder builder = KafkaError.newBuilder()
                .setContextMessage(contextMessage)
                .setOffset(context.offset())
                .setPartition(context.partition())
                .setTopic(topic)
                .setApplicationId(KafkaStreamsExecutionContext.getProperties().getProperty(APPLICATION_ID_CONFIG))
                .setProcessorNodeId(context.processorNodeId())
                .setTaskId(context.taskId().toString());

        return enrichWithException(builder, exception, key, value).build();
    }

    /**
     * Configures the handler.
     *
     * @param configs The configuration map
     */
    @Override
    public void configure(Map<String, ?> configs) {
        // Do nothing
    }
}
