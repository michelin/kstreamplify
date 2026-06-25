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

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.DEFINED_PORT;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.integration.container.KafkaIntegrationTest;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ActiveProfiles("dlq-processing-exception-handler")
@SpringBootTest(webEnvironment = DEFINED_PORT)
@AutoConfigureTestRestTemplate
class DlqProcessingExceptionHandlerIntegrationTest extends KafkaIntegrationTest {

    @BeforeAll
    static void globalSetUp() {
        createTopics(
                broker.getBootstrapServers(),
                new TopicPartition("STRING_TOPIC", 3),
                new TopicPartition("AVRO_TOPIC", 2));
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        waitingForKafkaStreamsToStart();
    }

    @Test
    void shouldSendRecordToDlqWhenProcessingFails() {
        ProducerRecord<String, String> message = new ProducerRecord<>("STRING_TOPIC", "user", "Doe");
        ProducerRecord<String, String> errorMessage1 =
                new ProducerRecord<>("STRING_TOPIC", "key-error-1", "Filter Exception");
        ProducerRecord<String, String> errorMessage2 =
                new ProducerRecord<>("STRING_TOPIC", "Map Exception", "value-error-2");
        List<ProducerRecord<String, String>> records =
                List.of(message, message, errorMessage1, message, message, errorMessage2, message);
        produceRecordToTopic(records, getKafkaGlobalProperties());

        Properties properties = getKafkaGlobalProperties();
        properties.put(GROUP_ID_CONFIG, "test-dlq-consumer-group");
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        List<ConsumerRecord<String, KafkaError>> dlqConsumerRecords =
                readAllRecordsFromTopic("DLQ_TOPIC", properties, 2);

        properties = getKafkaGlobalProperties();
        properties.put(GROUP_ID_CONFIG, "test-consumer-group");
        List<ConsumerRecord<String, String>> consumerRecords = readAllRecordsFromTopic("OUTPUT_TOPIC", properties, 5);
        assertEquals(5, consumerRecords.size());

        KafkaError kafkaError1 = dlqConsumerRecords.get(0).value();
        assertEquals(
                "An exception occurred during the stream processing of a record. Please find more details about the exception in the cause and stack fields.",
                kafkaError1.getContextMessage());
        assertEquals(2, kafkaError1.getOffset());
        assertEquals(2, kafkaError1.getPartition());
        assertEquals("STRING_TOPIC", kafkaError1.getTopic());
        assertEquals("appKeyValueProcessingExceptionHandlerId", kafkaError1.getApplicationId());
        assertEquals("filter-values", kafkaError1.getProcessorNodeId());
        assertEquals("0_2", kafkaError1.getTaskId());
        assertEquals("Exception while filtering values...", kafkaError1.getCause());
        assertTrue(kafkaError1.getStack().contains("java.lang.RuntimeException: Wrapper"));
        assertEquals("Filter Exception", new String(kafkaError1.getByteValue().array()));
        assertEquals("key-error-1", new String(kafkaError1.getSourceRawKey().array()));
        assertEquals(
                "Filter Exception", new String(kafkaError1.getSourceRawValue().array()));
        assertNull(kafkaError1.getValue());

        KafkaError kafkaError2 = dlqConsumerRecords.get(1).value();
        assertEquals(
                "An exception occurred during the stream processing of a record. Please find more details about the exception in the cause and stack fields.",
                kafkaError1.getContextMessage());
        assertEquals(0, kafkaError2.getOffset());
        assertEquals(0, kafkaError2.getPartition());
        assertEquals(
                "appKeyValueProcessingExceptionHandlerId-repartitioned-values-repartition", kafkaError2.getTopic());
        assertEquals("appKeyValueProcessingExceptionHandlerId", kafkaError2.getApplicationId());
        assertEquals("map-values", kafkaError2.getProcessorNodeId());
        assertEquals("1_0", kafkaError2.getTaskId());
        assertEquals("Exception while mapping values...", kafkaError2.getCause());
        assertTrue(kafkaError1.getStack().contains("java.lang.RuntimeException: Wrapper"));
        assertEquals(
                "transformed-value-error-2",
                new String(kafkaError2.getByteValue().array()));
        assertEquals("Map Exception", new String(kafkaError2.getSourceRawKey().array()));
        assertEquals("value-error-2", new String(kafkaError2.getSourceRawValue().array()));
        assertNull(kafkaError2.getValue());
    }

    /**
     * Kafka Streams starter implementation for integration tests. The topology consumes events from multiple topics
     * (string, Java, Avro) and stores them in dedicated stores so that they can be queried.
     */
    @SpringBootApplication
    static class KafkaStreamsStarterStub extends KafkaStreamsStarter {

        @Override
        public void topology(StreamsBuilder streamsBuilder) {
            streamsBuilder.stream("STRING_TOPIC", Consumed.with(Serdes.String(), Serdes.String()))
                    .filter(
                            (k, v) -> {
                                if (k.equals("key-error-1")) {
                                    throw new RuntimeException(
                                            "Wrapper", new KafkaException("Exception while filtering values..."));
                                }
                                return true;
                            },
                            Named.as("filter-values"))
                    .repartition(
                            Repartitioned.with(Serdes.String(), Serdes.String()).withName("repartitioned-values"))
                    .map(
                            (k, v) -> new KeyValue<>("transformed-" + k, "transformed-" + v),
                            Named.as("transformed-key-values"))
                    .mapValues(
                            (k, v) -> {
                                if (v.contains("value-error-2")) {
                                    throw new RuntimeException(
                                            "Wrapper", new KafkaException("Exception while mapping values..."));
                                }
                                return "new value is " + v;
                            },
                            Named.as("map-values"))
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
