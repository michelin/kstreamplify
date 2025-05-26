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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DlqDeserializationExceptionHandlerTest {
    @Mock
    private ErrorHandlerContext errorHandlerContext;

    @Mock
    private ConsumerRecord<byte[], byte[]> consumerRecord;

    private Producer<byte[], KafkaError> producer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        Serializer<KafkaError> serializer = (Serializer) new KafkaAvroSerializer();
        serializer.configure(Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"), false);
        producer = new MockProducer<>(true, new ByteArraySerializer(), serializer);

        KafkaStreamsExecutionContext.setDlqTopicName(null);
    }

    @Test
    void shouldReturnFailIfNoDlq() {
        DlqDeserializationExceptionHandler handler = new DlqDeserializationExceptionHandler(producer);

        DeserializationExceptionHandler.DeserializationHandlerResponse response =
                handler.handle(errorHandlerContext, consumerRecord, new RuntimeException("Exception..."));

        assertEquals(DeserializationExceptionHandler.DeserializationHandlerResponse.FAIL, response);
    }

    @Test
    void shouldReturnFailOnExceptionDuringHandle() {
        DlqDeserializationExceptionHandler handler = new DlqDeserializationExceptionHandler(producer);
        KafkaStreamsExecutionContext.setDlqTopicName("DLQ_TOPIC");

        DeserializationExceptionHandler.DeserializationHandlerResponse response =
                handler.handle(errorHandlerContext, consumerRecord, new KafkaException("Exception..."));

        assertEquals(DeserializationExceptionHandler.DeserializationHandlerResponse.FAIL, response);
    }

    @Test
    void shouldReturnContinueOnKafkaException() {
        DlqDeserializationExceptionHandler handler = new DlqDeserializationExceptionHandler(producer);
        KafkaStreamsExecutionContext.setDlqTopicName("DLQ_TOPIC");

        when(consumerRecord.key()).thenReturn("key".getBytes(StandardCharsets.UTF_8));
        when(consumerRecord.value()).thenReturn("value".getBytes(StandardCharsets.UTF_8));
        when(consumerRecord.topic()).thenReturn("topic");

        DeserializationExceptionHandler.DeserializationHandlerResponse response =
                handler.handle(errorHandlerContext, consumerRecord, new KafkaException("Exception..."));

        assertEquals(DeserializationExceptionHandler.DeserializationHandlerResponse.CONTINUE, response);
    }

    @Test
    void shouldConfigure() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("schema.registry.url", "localhost:8080");
        configs.put("acks", "all");

        DlqDeserializationExceptionHandler handler = new DlqDeserializationExceptionHandler();
        handler.configure(configs);

        assertNotNull(DlqExceptionHandler.getProducer());
    }

    @Test
    void shouldEnrichWithException() {
        KafkaError.Builder kafkaError = KafkaError.newBuilder()
                .setTopic("topic")
                .setStack("stack")
                .setPartition(0)
                .setOffset(0)
                .setCause("cause")
                .setValue("value");

        DlqDeserializationExceptionHandler handler = new DlqDeserializationExceptionHandler();
        KafkaError.Builder enrichedBuilder = handler.enrichWithException(
                kafkaError,
                new RuntimeException("Exception..."),
                "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8));

        KafkaError error = enrichedBuilder.build();
        assertEquals("Unknown cause", error.getCause());
        assertNull(error.getContextMessage());
    }

    @Test
    void shouldEnrichWithRecordTooLargeException() {
        KafkaError.Builder kafkaError = KafkaError.newBuilder()
                .setTopic("topic")
                .setStack("stack")
                .setPartition(0)
                .setOffset(0)
                .setCause("cause")
                .setValue("value");

        DlqDeserializationExceptionHandler handler = new DlqDeserializationExceptionHandler();
        KafkaError.Builder enrichedBuilder = handler.enrichWithException(
                kafkaError,
                new RecordTooLargeException("Exception..."),
                "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8));

        KafkaError error = enrichedBuilder.build();
        assertEquals("Unknown cause", error.getCause());
        assertEquals(
                "The record is too large to be set as value (5 bytes). " + "The key will be used instead",
                error.getValue());
    }
}
