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
package com.michelin.kstreamplify;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.error.ProcessingResult;
import com.michelin.kstreamplify.error.TopologyErrorHandler;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.serde.SerdesUtils;
import com.michelin.kstreamplify.serde.TopicWithSerde;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TopologyErrorHandlerTest extends KafkaStreamsStarterTest {
    private static final String AVRO_TOPIC = "avroTopic";
    private static final String STRING_TOPIC = "stringTopic";
    private static final String OUTPUT_AVRO_TOPIC = "outputAvroTopic";
    private static final String OUTPUT_STRING_TOPIC = "outputStringTopic";
    private static final String DLQ_TOPIC = "dlqTopic";

    private TestInputTopic<String, KafkaError> avroInputTopic;
    private TestInputTopic<String, String> stringInputTopic;
    private TestOutputTopic<String, KafkaError> avroOutputTopic;
    private TestOutputTopic<String, String> stringOutputTopic;
    private TestOutputTopic<String, KafkaError> dlqTopic;

    @Override
    protected KafkaStreamsStarter getKafkaStreamsStarter() {
        return new KafkaStreamsStarter() {
            @Override
            public String dlqTopic() {
                return DLQ_TOPIC;
            }

            @Override
            public void topology(StreamsBuilder streamsBuilder) {
                KStream<String, ProcessingResult<String, String>> stringStream = streamsBuilder.stream(
                                STRING_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                        .mapValues(value -> "error".equals(value)
                                ? ProcessingResult.fail(new NullPointerException(), value)
                                : ProcessingResult.success(value));

                TopologyErrorHandler.catchErrors(stringStream)
                        .to(OUTPUT_STRING_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

                KStream<String, ProcessingResult<KafkaError, KafkaError>> avroStream = streamsBuilder.stream(
                                AVRO_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.<KafkaError>getValueSerdes()))
                        .mapValues(value -> value == null
                                ? ProcessingResult.fail(new NullPointerException(), null)
                                : ProcessingResult.success(value));

                TopologyErrorHandler.catchErrors(avroStream)
                        .to(OUTPUT_AVRO_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
            }
        };
    }

    @BeforeEach
    void setUp() {
        stringInputTopic = testDriver.createInputTopic(STRING_TOPIC, new StringSerializer(), new StringSerializer());
        avroInputTopic = testDriver.createInputTopic(
                AVRO_TOPIC,
                new StringSerializer(),
                SerdesUtils.<KafkaError>getValueSerdes().serializer());

        stringOutputTopic =
                testDriver.createOutputTopic(OUTPUT_STRING_TOPIC, new StringDeserializer(), new StringDeserializer());
        avroOutputTopic = testDriver.createOutputTopic(
                OUTPUT_AVRO_TOPIC,
                new StringDeserializer(),
                SerdesUtils.<KafkaError>getValueSerdes().deserializer());

        dlqTopic = testDriver.createOutputTopic(
                DLQ_TOPIC,
                new StringDeserializer(),
                SerdesUtils.<KafkaError>getValueSerdes().deserializer());
    }

    @Test
    void shouldContinueWhenProcessingValueIsValid() {
        stringInputTopic.pipeInput("key", "message");

        var resultDlq = dlqTopic.readValuesToList();
        var resultOutput = stringOutputTopic.readValuesToList();

        assertEquals(0, resultDlq.size());
        assertEquals(1, resultOutput.size());
    }

    @Test
    void shouldSendExceptionToDlqWhenProcessingValueIsInvalid() {
        stringInputTopic.pipeInput("key", "error");

        var resultDlq = dlqTopic.readValuesToList();
        assertEquals(1, resultDlq.size());
        var record = resultDlq.get(0);
        assertEquals(record.getApplicationId(), "test");
        assertEquals(record.getTopic(), "stringTopic");
        assertEquals(record.getValue(), "error");

        var resultOutput = stringOutputTopic.readValuesToList();
        assertEquals(0, resultOutput.size());
    }

    @Test
    void shouldContinueWhenProcessingValueIsValidAvro() {
        KafkaError avroModel = KafkaError.newBuilder()
                .setTopic("topic")
                .setStack("stack")
                .setPartition(0)
                .setOffset(0)
                .setCause("cause")
                .setValue("value")
                .build();

        avroInputTopic.pipeInput("key", avroModel);

        List<KafkaError> resultDlq = dlqTopic.readValuesToList();
        List<KafkaError> resultOutput = avroOutputTopic.readValuesToList();

        assertEquals(0, resultDlq.size());
        assertEquals(1, resultOutput.size());
    }

    @Test
    void shouldContinueWhenProcessingValueIsInvalidAvro() {
        avroInputTopic.pipeInput("key", null);

        List<KafkaError> resultDlq = dlqTopic.readValuesToList();
        List<KafkaError> resultOutput = avroOutputTopic.readValuesToList();

        assertEquals(1, resultDlq.size());
        assertEquals(0, resultOutput.size());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldCreateInputAndOutputTopicsWithDeprecatedSerde() {
        TestInputTopic<String, String> inputTopic =
                createInputTestTopic(new com.michelin.kstreamplify.utils.TopicWithSerde<>(
                        "INPUT_TOPIC", "APP_NAME", Serdes.String(), Serdes.String()));

        assertEquals(
                "TestInputTopic[topic='INPUT_TOPIC', keySerializer=StringSerializer, "
                        + "valueSerializer=StringSerializer]",
                inputTopic.toString());

        TestOutputTopic<String, String> outputTopic =
                createOutputTestTopic(new com.michelin.kstreamplify.utils.TopicWithSerde<>(
                        "OUTPUT_TOPIC", "APP_NAME", Serdes.String(), Serdes.String()));

        assertEquals(
                "TestOutputTopic[topic='OUTPUT_TOPIC', keyDeserializer=StringDeserializer, "
                        + "valueDeserializer=StringDeserializer, size=0]",
                outputTopic.toString());
    }

    @Test
    void shouldCreateInputAndOutputTopicsWithSerde() {
        TestInputTopic<String, String> inputTopic =
                createInputTestTopic(new TopicWithSerde<>("INPUT_TOPIC", "APP_NAME", Serdes.String(), Serdes.String()));

        assertEquals(
                "TestInputTopic[topic='INPUT_TOPIC', keySerializer=StringSerializer, "
                        + "valueSerializer=StringSerializer]",
                inputTopic.toString());

        TestOutputTopic<String, String> outputTopic = createOutputTestTopic(
                new TopicWithSerde<>("OUTPUT_TOPIC", "APP_NAME", Serdes.String(), Serdes.String()));

        assertEquals(
                "TestOutputTopic[topic='OUTPUT_TOPIC', keyDeserializer=StringDeserializer, "
                        + "valueDeserializer=StringDeserializer, size=0]",
                outputTopic.toString());
    }

    /** Test the default storage path. */
    @Test
    void shouldValidateDefaultStorageDir() {
        Properties properties = KafkaStreamsExecutionContext.getProperties();
        assertEquals("/tmp/kafka-streams/" + getClass().getSimpleName(), properties.getProperty(STATE_DIR_CONFIG));
    }

    /** Test the default schema registry url. */
    @Test
    void shouldValidateDefaultSchemaRegistryUrl() {
        Properties properties = KafkaStreamsExecutionContext.getProperties();
        assertEquals("mock://" + getClass().getSimpleName(), properties.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
    }
}
