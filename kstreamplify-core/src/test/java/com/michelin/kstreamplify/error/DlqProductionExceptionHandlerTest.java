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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
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

    private Producer<byte[], KafkaError> producer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        Serializer<KafkaError> serializer = (Serializer) new KafkaAvroSerializer();
        serializer.configure(Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"), false);
        producer = new MockProducer<>(true, new ByteArraySerializer(), serializer);

        KafkaStreamsExecutionContext.setDlqTopicName(null);
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "test-app");
        KafkaStreamsExecutionContext.setProperties(properties);
    }

    @Test
    void shouldReturnFailIfNoDlq() {
        DlqProductionExceptionHandler handler = new DlqProductionExceptionHandler(producer);

        ProductionExceptionHandler.ProductionExceptionHandlerResponse response =
                handler.handle(errorHandlerContext, producerRecord, new RuntimeException("Exception..."));

        assertEquals(ProductionExceptionHandler.ProductionExceptionHandlerResponse.FAIL, response);
    }

    @Test
    void shouldReturnContinueOnExceptionDuringHandle() {
        DlqProductionExceptionHandler handler = new DlqProductionExceptionHandler(producer);
        KafkaStreamsExecutionContext.setDlqTopicName("DLQ_TOPIC");

        ProductionExceptionHandler.ProductionExceptionHandlerResponse response =
                handler.handle(errorHandlerContext, producerRecord, new KafkaException("Exception..."));

        assertEquals(ProductionExceptionHandler.ProductionExceptionHandlerResponse.CONTINUE, response);
    }

    @Test
    void shouldReturnContinueOnKafkaException() {
        DlqProductionExceptionHandler handler = new DlqProductionExceptionHandler(producer);
        KafkaStreamsExecutionContext.setDlqTopicName("DLQ_TOPIC");

        when(producerRecord.key()).thenReturn("key".getBytes(StandardCharsets.UTF_8));
        when(producerRecord.value()).thenReturn("value".getBytes(StandardCharsets.UTF_8));
        when(producerRecord.topic()).thenReturn("topic");

        ProductionExceptionHandler.ProductionExceptionHandlerResponse response =
                handler.handle(errorHandlerContext, producerRecord, new KafkaException("Exception..."));

        assertEquals(ProductionExceptionHandler.ProductionExceptionHandlerResponse.CONTINUE, response);
    }

    @Test
    void shouldReturnFailOnRetriableException() {
        DlqProductionExceptionHandler handler = new DlqProductionExceptionHandler(producer);
        KafkaStreamsExecutionContext.setDlqTopicName("DLQ_TOPIC");

        ProductionExceptionHandler.ProductionExceptionHandlerResponse response =
                handler.handle(errorHandlerContext, producerRecord, new RetriableCommitFailedException("Exception..."));

        assertEquals(ProductionExceptionHandler.ProductionExceptionHandlerResponse.FAIL, response);
    }

    @Test
    void shouldConfigure() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("schema.registry.url", "localhost:8080");
        configs.put("acks", "all");

        DlqProductionExceptionHandler handler = new DlqProductionExceptionHandler();
        handler.configure(configs);

        assertNotNull(DlqExceptionHandler.getProducer());
    }
}
