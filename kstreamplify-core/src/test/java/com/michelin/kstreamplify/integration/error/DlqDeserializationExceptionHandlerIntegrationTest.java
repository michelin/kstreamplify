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
package com.michelin.kstreamplify.integration.error;

import static com.michelin.kstreamplify.property.PropertiesUtils.KAFKA_PROPERTIES_PREFIX;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.integration.container.KafkaIntegrationTest;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class DlqDeserializationExceptionHandlerIntegrationTest extends KafkaIntegrationTest {

    @BeforeAll
    static void globalSetUp() {
        createTopics(
                broker.getBootstrapServers(),
                new TopicPartition("INPUT_TOPIC", 3),
                new TopicPartition("OUTPUT_TOPIC", 3),
                new TopicPartition("DLQ_TOPIC", 3));

        Properties properties = getKafkaStreamProperties();
        properties.putAll(Map.of(
                KAFKA_PROPERTIES_PREFIX + APPLICATION_ID_CONFIG,
                "appKeyValueDeserializationExceptionHandlerId",
                KAFKA_PROPERTIES_PREFIX + DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                "com.michelin.kstreamplify.error.DlqDeserializationExceptionHandler"));

        initializer = new KafkaStreamInitializerStub(new KafkaStreamsStarterStub(), 8087, properties);

        initializer.start();
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        waitingForKafkaStreamsToStart();
    }

    @Test
    void shouldSendRecordToDlqWhenValueDeserializationFails() {
        Properties properties = getKafkaGlobalProperties();
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        ProducerRecord<String, byte[]> message = new ProducerRecord<>(
                "INPUT_TOPIC",
                "key",
                ByteBuffer.allocate(Long.BYTES).putLong(1L).array());
        ProducerRecord<String, byte[]> error =
                new ProducerRecord<>("INPUT_TOPIC", "key-error", "error-value".getBytes());

        produceRecordToTopic(List.of(message, error, message), properties);

        properties.put(GROUP_ID_CONFIG, "test-dlq-consumer-group");
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        List<ConsumerRecord<String, KafkaError>> dlqConsumerRecords =
                readAllRecordsFromTopic("DLQ_TOPIC", properties, 1);

        properties.put(GROUP_ID_CONFIG, "test-consumer-group");
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        List<ConsumerRecord<String, String>> consumerRecords = readAllRecordsFromTopic("OUTPUT_TOPIC", properties, 2);
        assertEquals(2, consumerRecords.size());

        KafkaError kafkaError = dlqConsumerRecords.get(0).value();
        assertEquals(
                "An exception occurred during the stream internal deserialization. Please find more details about the exception in the cause and stack fields.",
                kafkaError.getContextMessage());
        assertEquals(1, kafkaError.getOffset());
        assertEquals(1, kafkaError.getPartition());
        assertEquals("INPUT_TOPIC", kafkaError.getTopic());
        assertEquals("appKeyValueDeserializationExceptionHandlerId", kafkaError.getApplicationId());
        assertEquals("source-processor", kafkaError.getProcessorNodeId());
        assertEquals("0_1", kafkaError.getTaskId());
        assertEquals("Unknown cause", kafkaError.getCause());
        assertTrue(kafkaError.getStack().contains("org.apache.kafka.common.errors.SerializationException"));
        assertEquals("error-value", new String(kafkaError.getByteValue().array()));
        assertEquals("key-error", new String(kafkaError.getSourceRawKey().array()));
        assertEquals("error-value", new String(kafkaError.getSourceRawValue().array()));
        assertNull(kafkaError.getValue());
    }

    /**
     * Kafka Streams starter implementation for integration tests. The topology consumes events from multiple topics
     * (string, Java, Avro) and stores them in dedicated stores so that they can be queried.
     */
    @Slf4j
    static class KafkaStreamsStarterStub extends KafkaStreamsStarter {
        @Override
        public void topology(StreamsBuilder streamsBuilder) {
            streamsBuilder.stream(
                            "INPUT_TOPIC",
                            Consumed.with(Serdes.String(), Serdes.Long()).withName("source-processor"))
                    .mapValues(String::valueOf, Named.as("transformed-key-values"))
                    .to("OUTPUT_TOPIC", Produced.with(Serdes.String(), Serdes.String()));
        }

        @Override
        public String dlqTopic() {
            return "DLQ_TOPIC";
        }

        @Override
        public void onStart(KafkaStreams kafkaStreams) {
            kafkaStreams.cleanUp();
        }
    }
}
