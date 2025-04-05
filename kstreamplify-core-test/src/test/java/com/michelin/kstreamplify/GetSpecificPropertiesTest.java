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

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.serde.TopicWithSerde;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.Test;

class GetSpecificPropertiesTest extends KafkaStreamsStarterTest {
    private static final String DLQ_TOPIC = "dlqTopic";
    private static final String SPECIFIC_STORAGE_PATH = "/tmp/personal-path";
    private static final String SPECIFIC_SCHEMA_REGISTRY_URL = "mock://specific-schema-registry-url";
    private static final String INPUT_TOPIC = "INPUT_TOPIC";
    private static final String OUTPUT_TOPIC = "OUTPUT_TOPIC";
    private static final String SELF_TOPIC = "SELF_TOPIC";

    @Override
    protected KafkaStreamsStarter getKafkaStreamsStarter() {
        return new KafkaStreamsStarter() {
            @Override
            public String dlqTopic() {
                return DLQ_TOPIC;
            }

            @Override
            public void topology(StreamsBuilder streamsBuilder) {
                // Do nothing
            }
        };
    }

    /**
     * Overrides the default properties with specific values for the test.
     *
     * @return a map containing the overridden properties
     */
    @Override
    protected Map<String, String> getSpecificProperties() {
        return Map.of(
                STATE_DIR_CONFIG,
                SPECIFIC_STORAGE_PATH,
                SCHEMA_REGISTRY_URL_CONFIG,
                SPECIFIC_SCHEMA_REGISTRY_URL,
                "prefix.abc",
                "abc.",
                "prefix.def",
                "def.");
    }

    /** Test when default properties are overridden. */
    @Test
    void shouldValidateOverriddenProperties() {
        Properties properties = KafkaStreamsExecutionContext.getProperties();
        assertEquals(SPECIFIC_STORAGE_PATH, properties.getProperty(STATE_DIR_CONFIG));
        assertEquals(SPECIFIC_SCHEMA_REGISTRY_URL, properties.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
    }

    /** Test to verify that input and output topics are created with the correct prefixes. */
    @Test
    void shouldCreateInputAndOutputTopicsWithPrefixes() {
        var inputTopicWithSerde = new TopicWithSerde<>(INPUT_TOPIC, "abc", Serdes.String(), Serdes.String());
        var outputTopicWithSerde = new TopicWithSerde<>(OUTPUT_TOPIC, "def", Serdes.String(), Serdes.String());
        var selfTopicWithSerde = new TopicWithSerde<>(SELF_TOPIC, Serdes.String(), Serdes.String());

        var inputTopic = createInputTestTopic(inputTopicWithSerde);
        var outputTopic = createOutputTestTopic(outputTopicWithSerde);
        var selfTopic = createInputTestTopic(selfTopicWithSerde);

        assertEquals(
                "TestInputTopic[topic='abc.INPUT_TOPIC', keySerializer=StringSerializer, "
                        + "valueSerializer=StringSerializer]",
                inputTopic.toString());
        assertEquals(
                "TestOutputTopic[topic='def.OUTPUT_TOPIC', keyDeserializer=StringDeserializer, "
                        + "valueDeserializer=StringDeserializer, size=0]",
                outputTopic.toString());
        assertEquals(
                "TestInputTopic[topic='SELF_TOPIC', keySerializer=StringSerializer, "
                        + "valueSerializer=StringSerializer]",
                selfTopic.toString());
    }
}
