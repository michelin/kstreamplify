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
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.integration.container.KafkaIntegrationTest;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
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
class DlqProductionExceptionHandlerIntegrationTest extends KafkaIntegrationTest {

    @BeforeAll
    static void globalSetUp() {
        createTopics(
                broker.getBootstrapServers(), new TopicPartition("INPUT_TOPIC", 3), new TopicPartition("DLQ_TOPIC", 3));

        createTopics(
                broker.getBootstrapServers(),
                Map.of(
                        "max.message.bytes", "500" // 1 MB
                        ),
                new TopicPartition("OUTPUT_TOPIC", 3));

        Properties properties = getKafkaStreamProperties();
        properties.putAll(Map.of(
                KAFKA_PROPERTIES_PREFIX + APPLICATION_ID_CONFIG,
                "appKeyValueProductionExceptionHandlerId",
                KAFKA_PROPERTIES_PREFIX + PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                "com.michelin.kstreamplify.error.DlqProductionExceptionHandler",
                KAFKA_PROPERTIES_PREFIX + RETRIES_CONFIG,
                "1"));

        initializer = new KafkaStreamInitializerStub(new KafkaStreamsStarterStub(), 8089, properties);

        initializer.start();
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        waitingForKafkaStreamsToStart();
    }

    @Test
    void shouldSendRecordToDlqWhenProductionFails() throws ExecutionException, InterruptedException {
        Properties properties = getKafkaGlobalProperties();
        ProducerRecord<String, String> message = new ProducerRecord<>("INPUT_TOPIC", "key", "value");
        ProducerRecord<String, String> error = new ProducerRecord<>("INPUT_TOPIC", "key-error", "value-error");
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
                "An exception occurred during the stream internal production. Please find more details about the exception in the cause and stack fields.",
                kafkaError.getContextMessage());
        assertEquals(1, kafkaError.getOffset());
        assertEquals(1, kafkaError.getPartition());
        assertEquals("OUTPUT_TOPIC", kafkaError.getTopic());
        assertEquals("appKeyValueProductionExceptionHandlerId", kafkaError.getApplicationId());
        assertEquals("sink-processor", kafkaError.getProcessorNodeId());
        assertEquals("0_1", kafkaError.getTaskId());
        assertEquals("Unknown cause", kafkaError.getCause());
        assertTrue(kafkaError.getStack().contains("org.apache.kafka.common.errors.RecordTooLargeException"));
        assertEquals("key-error", new String(kafkaError.getByteValue().array()));
        assertEquals(
                "The record is too large to be set as value (1048588 bytes). The key will be used instead",
                kafkaError.getValue());
        assertNull(kafkaError.getSourceRawKey());
        assertNull(kafkaError.getSourceRawValue());
    }

    /**
     * Kafka Streams starter implementation for integration tests. The topology consumes events from multiple topics
     * (string, Java, Avro) and stores them in dedicated stores so that they can be queried.
     */
    @Slf4j
    static class KafkaStreamsStarterStub extends KafkaStreamsStarter {
        @Override
        public void topology(StreamsBuilder streamsBuilder) {
            streamsBuilder.stream("INPUT_TOPIC", Consumed.with(Serdes.String(), Serdes.String()))
                    .mapValues(
                            (v) -> {
                                if (v.equals("value-error")) {
                                    int recordSize = 1048588;
                                    char[] chars = new char[recordSize];
                                    Arrays.fill(chars, 'A'); // fill with 'A'
                                    return new String(chars);
                                }
                                return "transformed-" + v;
                            },
                            Named.as("transformed-values"))
                    .to(
                            "OUTPUT_TOPIC",
                            Produced.with(Serdes.String(), Serdes.String()).withName("sink-processor"));
            ;
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
