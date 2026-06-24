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

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.serde.SerdesUtils;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The class managing processing exceptions. */
public class DlqProcessingExceptionHandler extends DlqExceptionHandler implements ProcessingExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(DlqProcessingExceptionHandler.class);

    /** Constructor. */
    public DlqProcessingExceptionHandler() {}

    /**
     * Handles processing exceptions by routing the record to the DLQ topic.
     *
     * @param context The error handler context
     * @param processingRecord The record that failed processing
     * @param exception The exception that occurred
     * @return A {@link Response} indicating how to proceed
     */
    @Override
    public Response handleError(ErrorHandlerContext context, Record<?, ?> processingRecord, Exception exception) {
        log.warn(
                "Exception during processing, processor node: {}, taskId: {}, topic: {}, partition: {}, offset: {}",
                context.processorNodeId(),
                context.taskId(),
                context.topic(),
                context.partition(),
                context.offset(),
                exception);

        if (isDlqNotDefined()) {
            log.warn("Failed to route processing error to DLQ. Define a DLQ topic in configuration.");
            return Response.fail();
        }

        try {
            KafkaError error = buildKafkaError(context, processingRecord, exception);
            Serde<KafkaError> serde = SerdesUtils.getValueSerdes();
            byte[] value = serde.serializer().serialize(KafkaStreamsExecutionContext.getDlqTopicName(), error);

            byte[] key = processingRecord.key() != null
                    ? processingRecord.key().toString().getBytes()
                    : null;

            return Response.resume(
                    List.of(new ProducerRecord<>(KafkaStreamsExecutionContext.getDlqTopicName(), key, value)));
        } catch (Exception e) {
            log.error(
                    "Cannot send processing exception to DLQ topic {}",
                    KafkaStreamsExecutionContext.getDlqTopicName(),
                    e);
            return Response.fail();
        }
    }

    /**
     * Builds a {@link KafkaError} from the record metadata and exception details.
     *
     * @param context The error handler context
     * @param processingRecord The record that failed processing
     * @param exception The exception that occurred
     * @return The built {@link KafkaError}
     */
    private KafkaError buildKafkaError(
            ErrorHandlerContext context, Record<?, ?> processingRecord, Exception exception) {

        KafkaError.Builder builder = KafkaError.newBuilder()
                .setContextMessage(
                        "An exception occurred during the stream processing of a record. Please find more details about the exception in the cause and stack fields.")
                .setOffset(context.offset())
                .setPartition(context.partition())
                .setTopic(context.topic())
                .setApplicationId(KafkaStreamsExecutionContext.getProperties().getProperty(APPLICATION_ID_CONFIG))
                .setProcessorNodeId(context.processorNodeId())
                .setTaskId(context.taskId().toString())
                .setSourceRawKey(ByteBuffer.wrap(context.sourceRawKey()))
                .setSourceRawValue(ByteBuffer.wrap(context.sourceRawValue()))
                .setValue(
                        processingRecord.value() == null
                                ? null
                                : processingRecord.value().toString());

        return enrichWithException(
                        builder,
                        exception,
                        processingRecord.key() != null
                                ? processingRecord.key().toString().getBytes()
                                : null,
                        processingRecord.value() != null
                                ? processingRecord.value().toString().getBytes()
                                : null)
                .build();
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
