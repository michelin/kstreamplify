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
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

/** The class managing DLQ production exceptions. */
@Slf4j
public class DlqProductionExceptionHandler extends DlqExceptionHandler implements ProductionExceptionHandler {

    /** Constructor. */
    public DlqProductionExceptionHandler() {}

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

    /** {@inheritDoc} */
    @Override
    public void configure(Map<String, ?> configs) {
        deadLetterQueueTopic = KafkaStreamsExecutionContext.getDlqTopicName();
    }
}
