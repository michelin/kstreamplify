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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.serde.SerdesUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DlqProcessingExceptionHandlerTest {
    @Mock
    private ErrorHandlerContext errorHandlerContext;

    @Mock
    private Record<String, String> record;

    @BeforeEach
    void setUp() {
        KafkaStreamsExecutionContext.setDlqTopicName(null);
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "test-app");
        properties.setProperty(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://");
        KafkaStreamsExecutionContext.registerProperties(properties);
        KafkaStreamsExecutionContext.setSerdesConfig(
                Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"));
    }

    @Test
    void shouldReturnFailIfNoDlq() {
        DlqProcessingExceptionHandler handler = new DlqProcessingExceptionHandler();

        DlqProcessingExceptionHandler.Response response =
                handler.handleError(errorHandlerContext, record, new RuntimeException("Exception..."));

        assertEquals(ProcessingExceptionHandler.Result.FAIL, response.result());
        assertTrue(response.deadLetterQueueRecords().isEmpty());
    }

    @Test
    void shouldReturnFailOnExceptionDuringHandle() {
        DlqProcessingExceptionHandler handler = new DlqProcessingExceptionHandler();
        KafkaStreamsExecutionContext.setDlqTopicName("DLQ_TOPIC");
        handler.configure(Map.of());

        DlqProcessingExceptionHandler.Response response =
                handler.handleError(errorHandlerContext, record, new RuntimeException("Exception..."));

        assertEquals(ProcessingExceptionHandler.Result.FAIL, response.result());
        assertTrue(response.deadLetterQueueRecords().isEmpty());
    }

    @Test
    void shouldReturnContinueOnKafkaException() {
        DlqProcessingExceptionHandler handler = new DlqProcessingExceptionHandler();
        KafkaStreamsExecutionContext.setDlqTopicName("DLQ_TOPIC");
        handler.configure(Map.of());

        when(record.key()).thenReturn("key");
        when(record.value()).thenReturn("value");
        when(errorHandlerContext.topic()).thenReturn("topic");
        when(errorHandlerContext.taskId()).thenReturn(new TaskId(0, 0));
        when(errorHandlerContext.partition()).thenReturn(0);
        when(errorHandlerContext.offset()).thenReturn(0L);
        when(errorHandlerContext.processorNodeId()).thenReturn("processorNode");
        when(errorHandlerContext.sourceRawKey()).thenReturn("sourceKey".getBytes(StandardCharsets.UTF_8));
        when(errorHandlerContext.sourceRawValue()).thenReturn("sourceValue".getBytes(StandardCharsets.UTF_8));

        // Wrap the KafkaException so that getCause() instanceof KafkaException
        Exception wrapped = new Exception("Wrapper", new KafkaException("Exception..."));

        ProcessingExceptionHandler.Response response = handler.handleError(errorHandlerContext, record, wrapped);

        assertEquals(ProcessingExceptionHandler.Result.RESUME, response.result());

        Serde<KafkaError> serde = SerdesUtils.getValueSerdes();
        KafkaError kafkaError = serde.deserializer()
                .deserialize(
                        "DLQ_TOPIC", response.deadLetterQueueRecords().get(0).value());

        assertEquals(
                "An exception occurred during the stream processing of a record. Please find more details about the exception in the cause and stack fields.",
                kafkaError.getContextMessage());
        assertEquals(0, kafkaError.getOffset());
        assertEquals(0, kafkaError.getPartition());
        assertEquals("topic", kafkaError.getTopic());
        assertEquals("test-app", kafkaError.getApplicationId());
        assertEquals("processorNode", kafkaError.getProcessorNodeId());
        assertEquals("0_0", kafkaError.getTaskId());
        assertEquals("Exception...", kafkaError.getCause());
        assertTrue(kafkaError.getStack().contains("Caused by: org.apache.kafka.common.KafkaException: Exception..."));
        assertEquals("key", new String(response.deadLetterQueueRecords().get(0).key()));
    }
}
