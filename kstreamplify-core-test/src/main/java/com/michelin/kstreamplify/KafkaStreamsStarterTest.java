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
import static org.apache.kafka.streams.StreamsConfig.*;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.serde.SerdesUtils;
import com.michelin.kstreamplify.serde.TopicWithSerde;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * The main test class to extend to execute unit tests on topology. It provides a {@link TopologyTestDriver} and a
 * {@link TestOutputTopic} for the DLQ.
 */
public abstract class KafkaStreamsStarterTest {
    private static final String STATE_DIR = "/tmp/kafka-streams/";

    /** The topology test driver. */
    protected TopologyTestDriver testDriver;

    /** The DLQ topic. */
    protected TestOutputTopic<String, KafkaError> dlqTopic;

    /** Constructor. */
    protected KafkaStreamsStarterTest() {}

    /** Set up topology test driver. */
    @BeforeEach
    void generalSetUp() {
        Properties properties = getProperties();

        KafkaStreamsExecutionContext.registerProperties(properties);

        String schemaRegistryUrl = properties.getProperty(SCHEMA_REGISTRY_URL_CONFIG);
        KafkaStreamsExecutionContext.setSerdesConfig(
                Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl));

        KafkaStreamsStarter starter = getKafkaStreamsStarter();

        KafkaStreamsExecutionContext.setDlqTopicName(starter.dlqTopic());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        starter.topology(streamsBuilder);

        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, getInitialWallClockTime());

        dlqTopic = testDriver.createOutputTopic(
                KafkaStreamsExecutionContext.getDlqTopicName(),
                new StringDeserializer(),
                SerdesUtils.<KafkaError>getValueSerdes().deserializer());
    }

    /**
     * Get additional properties for the tests.
     *
     * @return The properties
     */
    private Properties getProperties() {
        Properties properties = new Properties();

        properties.setProperty(APPLICATION_ID_CONFIG, "test");
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "mock:1234");
        properties.setProperty(STATE_DIR_CONFIG, STATE_DIR + getClass().getSimpleName());
        properties.setProperty(
                SCHEMA_REGISTRY_URL_CONFIG, "mock://" + getClass().getSimpleName());

        Map<String, String> propertiesMap = getSpecificProperties();
        if (propertiesMap != null && !propertiesMap.isEmpty()) {
            properties.putAll(propertiesMap);
        }

        return properties;
    }

    /**
     * Provide the KafkaStreamsStarter to test.
     *
     * @return The KafkaStreamsStarter to test
     */
    protected abstract KafkaStreamsStarter getKafkaStreamsStarter();

    /**
     * Default wall clock time for topology test driver.
     *
     * @return The default wall clock time
     */
    protected Instant getInitialWallClockTime() {
        return Instant.ofEpochMilli(1577836800000L);
    }

    /**
     * Provide specific properties to add or override for the tests.
     *
     * @return A map of properties
     */
    protected Map<String, String> getSpecificProperties() {
        return Collections.emptyMap();
    }

    /** Close everything after each test. */
    @AfterEach
    protected void generalTearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(
                Path.of(KafkaStreamsExecutionContext.getProperties().getProperty(STATE_DIR_CONFIG)));
        MockSchemaRegistry.dropScope("mock://" + getClass().getSimpleName());
    }

    /**
     * Creates an input test topic on the testDriver using the provided topicWithSerde.
     *
     * @param topicWithSerde The topic with serde used to create the test topic
     * @param <K> The serializable type of the key
     * @param <V> The serializable type of the value
     * @return The corresponding TestInputTopic
     */
    protected <K, V> TestInputTopic<K, V> createInputTestTopic(TopicWithSerde<K, V> topicWithSerde) {
        return testDriver.createInputTopic(
                topicWithSerde.toString(),
                topicWithSerde.getKeySerde().serializer(),
                topicWithSerde.getValueSerde().serializer());
    }

    /**
     * Creates an input test topic on the testDriver using the provided topicWithSerde.
     *
     * @param topicWithSerde The topic with serde used to create the test topic
     * @param <K> The serializable type of the key
     * @param <V> The serializable type of the value
     * @return The corresponding TestInputTopic
     * @deprecated Use {@link #createInputTestTopic(TopicWithSerde)}
     */
    @Deprecated(since = "1.1.0")
    protected <K, V> TestInputTopic<K, V> createInputTestTopic(
            com.michelin.kstreamplify.utils.TopicWithSerde<K, V> topicWithSerde) {
        return createInputTestTopic(new TopicWithSerde<>(
                topicWithSerde.getUnPrefixedName(), topicWithSerde.getKeySerde(), topicWithSerde.getValueSerde()));
    }

    /**
     * Creates an output test topic on the testDriver using the provided topicWithSerde.
     *
     * @param topicWithSerde The topic with serde used to create the test topic
     * @param <K> The serializable type of the key
     * @param <V> The serializable type of the value
     * @return The corresponding TestOutputTopic
     */
    protected <K, V> TestOutputTopic<K, V> createOutputTestTopic(TopicWithSerde<K, V> topicWithSerde) {
        return this.testDriver.createOutputTopic(
                topicWithSerde.toString(),
                topicWithSerde.getKeySerde().deserializer(),
                topicWithSerde.getValueSerde().deserializer());
    }

    /**
     * Creates an output test topic on the testDriver using the provided topicWithSerde.
     *
     * @param topicWithSerde The topic with serde used to create the test topic
     * @param <K> The serializable type of the key
     * @param <V> The serializable type of the value
     * @return The corresponding TestOutputTopic
     * @deprecated Use {@link #createOutputTestTopic(TopicWithSerde)}
     */
    @Deprecated(since = "1.1.0")
    protected <K, V> TestOutputTopic<K, V> createOutputTestTopic(
            com.michelin.kstreamplify.utils.TopicWithSerde<K, V> topicWithSerde) {
        return createOutputTestTopic(new TopicWithSerde<>(
                topicWithSerde.getUnPrefixedName(), topicWithSerde.getKeySerde(), topicWithSerde.getValueSerde()));
    }
}
