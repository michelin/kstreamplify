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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.serde.SerdesUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DlqProductionExceptionHandlerTest {
    @Mock
    private ErrorHandlerContext errorHandlerContext;

    @Mock
    private ProducerRecord<byte[], byte[]> producerRecord;

    @BeforeEach
    @SuppressWarnings("unchecked")
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
        DlqProductionExceptionHandler handler = new DlqProductionExceptionHandler();

        ProductionExceptionHandler.Response response =
                handler.handleError(errorHandlerContext, producerRecord, new RuntimeException("Exception..."));

        assertEquals(ProductionExceptionHandler.Result.FAIL, response.result());
        assertTrue(response.deadLetterQueueRecords().isEmpty());
    }

    @Test
    void shouldReturnContinueOnExceptionDuringHandle() {
        DlqProductionExceptionHandler handler = new DlqProductionExceptionHandler();
        KafkaStreamsExecutionContext.setDlqTopicName("DLQ_TOPIC");
        handler.configure(Map.of());

        ProductionExceptionHandler.Response response =
                handler.handleError(errorHandlerContext, producerRecord, new KafkaException("Exception..."));

        assertEquals(ProductionExceptionHandler.Result.RESUME, response.result());
        assertTrue(response.deadLetterQueueRecords().isEmpty());
    }

    @Test
    void shouldReturnContinueOnKafkaException() {
        DlqProductionExceptionHandler handler = new DlqProductionExceptionHandler();
        KafkaStreamsExecutionContext.setDlqTopicName("DLQ_TOPIC");
        handler.configure(Map.of());

        when(producerRecord.key()).thenReturn("key".getBytes(StandardCharsets.UTF_8));
        when(producerRecord.value()).thenReturn("value".getBytes(StandardCharsets.UTF_8));
        when(producerRecord.topic()).thenReturn("topic");
        when(errorHandlerContext.taskId()).thenReturn(new TaskId(0, 0));
        when(errorHandlerContext.partition()).thenReturn(0);

        // Wrap the KafkaException so that getCause() instanceof KafkaException
        Exception wrapped = new Exception("Wrapper", new KafkaException("Exception..."));

        ProductionExceptionHandler.Response response =
                handler.handleError(errorHandlerContext, producerRecord, wrapped);

        assertEquals(ProductionExceptionHandler.Result.RESUME, response.result());

        Serde<KafkaError> serde = SerdesUtils.getValueSerdes();
        KafkaError kafkaError = serde.deserializer()
                .deserialize(
                        "DLQ_TOPIC", response.deadLetterQueueRecords().get(0).value());

        assertEquals(
                "An exception occurred during the stream internal production. Please find more details about the exception in the cause and stack fields.",
                kafkaError.getContextMessage());
        assertEquals(0, kafkaError.getOffset());
        assertEquals(0, kafkaError.getPartition());
        assertEquals("topic", kafkaError.getTopic());
        assertEquals("test-app", kafkaError.getApplicationId());
        assertNull(kafkaError.getProcessorNodeId());
        assertEquals("0_0", kafkaError.getTaskId());
        assertEquals("Exception...", kafkaError.getCause());
        assertTrue(kafkaError.getStack().contains("Caused by: org.apache.kafka.common.KafkaException: Exception..."));
        assertEquals("key", new String(response.deadLetterQueueRecords().get(0).key()));
    }

    @Test
    void shouldReturnRetryOnRetriableException() {
        DlqProductionExceptionHandler handler = new DlqProductionExceptionHandler();
        KafkaStreamsExecutionContext.setDlqTopicName("DLQ_TOPIC");
        handler.configure(Map.of());

        ProductionExceptionHandler.Response response = handler.handleError(
                errorHandlerContext, producerRecord, new RetriableCommitFailedException("Exception..."));

        assertEquals(ProductionExceptionHandler.Result.RETRY, response.result());
        assertTrue(response.deadLetterQueueRecords().isEmpty());
    }
}
