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
package com.michelin.kstreamplify.test;

import static org.junit.jupiter.api.Assertions.fail;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.serde.TopicWithSerde;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;

/**
 * Fluent entry point for the Kstreamplify testing DSL. It provides a {@code Given → When → Then} API on top of the
 * existing {@link TopologyTestDriver} infrastructure while preserving access to the underlying driver for advanced use
 * cases.
 *
 * <p>A context is bound to a single test method. The records read from an output topic or from the dead letter queue
 * are accumulated by the context, so calling {@code then(...)} or {@code thenDlq()} several times in the same test
 * always sees all the records produced since the beginning of the test.
 */
public final class KstreamplifyTestContext {
    private final TopologyTestDriver testDriver;
    private final TestOutputTopic<String, KafkaError> dlqTopic;
    private final Map<String, List<TestRecord<?, ?>>> readRecords = new HashMap<>();
    private final List<TestRecord<String, KafkaError>> readDlqRecords = new ArrayList<>();
    private Instant currentTime;

    /**
     * Constructor.
     *
     * @param testDriver The underlying topology test driver
     * @param dlqTopic The DLQ output topic created by the test infrastructure
     * @param initialTime The initial event time used for the records piped without an explicit timestamp
     */
    public KstreamplifyTestContext(
            TopologyTestDriver testDriver, TestOutputTopic<String, KafkaError> dlqTopic, Instant initialTime) {
        this.testDriver = testDriver;
        this.dlqTopic = dlqTopic;
        this.currentTime = initialTime;
    }

    /**
     * Start a {@code given} stage that feeds records to the provided input topic.
     *
     * @param topic The input topic to feed
     * @param <K> The type of the key
     * @param <V> The type of the value
     * @return A {@link GivenStage} bound to the provided topic
     */
    public <K, V> GivenStage<K, V> given(TopicWithSerde<K, V> topic) {
        return new GivenStage<>(this, topic);
    }

    /**
     * Mark the point at which the supplied inputs have been processed by the topology.
     *
     * @return A {@link WhenStage} to chain output, DLQ or state store assertions
     */
    public WhenStage when() {
        return new WhenStage(this);
    }

    /**
     * Start typed assertions on the records produced on the provided output topic.
     *
     * @param topic The output topic to assert on
     * @param <K> The type of the key
     * @param <V> The type of the value
     * @return An {@link OutputAssertion} holding the produced records
     */
    public <K, V> OutputAssertion<K, V> then(TopicWithSerde<K, V> topic) {
        return new OutputAssertion<>(this, topic.toString(), readOutputRecords(topic));
    }

    /**
     * Start assertions on the records sent to the dead letter queue.
     *
     * @return A {@link DlqAssertion} holding the DLQ records
     */
    public DlqAssertion thenDlq() {
        readDlqRecords.addAll(dlqTopic.readRecordsToList());
        return new DlqAssertion(this, KafkaStreamsExecutionContext.getDlqTopicName(), List.copyOf(readDlqRecords));
    }

    /**
     * Start assertions on the content of the provided key-value state store.
     *
     * @param storeName The name of the state store
     * @param <K> The type of the key
     * @param <V> The type of the value
     * @return A {@link StateStoreAssertion} bound to the state store
     */
    public <K, V> StateStoreAssertion<K, V> thenStateStore(String storeName) {
        KeyValueStore<K, V> store = testDriver.getKeyValueStore(storeName);
        if (store == null) {
            fail(String.format(
                    "No key-value state store named '%s' in the topology.%nAvailable state stores:%n%s",
                    storeName, formatStateStoreNames()));
        }
        return new StateStoreAssertion<>(this, storeName, store);
    }

    /**
     * Advance the event time used by the records piped afterward without an explicit timestamp. As defined by Kafka
     * Streams, it does not advance the wall clock time of the topology test driver.
     *
     * @param duration The duration to advance the event time by
     * @return This context for chaining
     */
    public KstreamplifyTestContext advanceTime(Duration duration) {
        this.currentTime = this.currentTime.plus(duration);
        return this;
    }

    /**
     * Advance the wall clock time of the topology test driver to trigger the wall-clock-time punctuators. As defined by
     * Kafka Streams, it does not advance the event time used by the records piped afterward.
     *
     * @param duration The duration to advance the wall clock time by
     * @return This context for chaining
     */
    public KstreamplifyTestContext advanceWallClockTime(Duration duration) {
        testDriver.advanceWallClockTime(duration);
        return this;
    }

    /**
     * Expose the underlying topology test driver as an escape hatch for advanced tests.
     *
     * @return The underlying topology test driver
     */
    public TopologyTestDriver driver() {
        return testDriver;
    }

    /**
     * Get the event time used by the records piped without an explicit timestamp.
     *
     * @return The current event time
     */
    Instant currentTime() {
        return currentTime;
    }

    /**
     * Set the event time used by the records piped without an explicit timestamp.
     *
     * @param instant The new current event time
     */
    void currentTime(Instant instant) {
        this.currentTime = instant;
    }

    /**
     * Create the test input topic backing the provided topic.
     *
     * @param topic The topic to resolve
     * @param <K> The type of the key
     * @param <V> The type of the value
     * @return The newly created test input topic
     */
    <K, V> TestInputTopic<K, V> inputTopic(TopicWithSerde<K, V> topic) {
        return testDriver.createInputTopic(
                topic.toString(),
                topic.getKeySerde().serializer(),
                topic.getValueSerde().serializer());
    }

    /**
     * Read the records produced on the provided topic since the last read and return all the records produced since the
     * beginning of the test.
     *
     * @param topic The topic to read
     * @param <K> The type of the key
     * @param <V> The type of the value
     * @return All the records produced on the topic since the beginning of the test
     */
    @SuppressWarnings("unchecked")
    private <K, V> List<TestRecord<K, V>> readOutputRecords(TopicWithSerde<K, V> topic) {
        TestOutputTopic<K, V> outputTopic = testDriver.createOutputTopic(
                topic.toString(),
                topic.getKeySerde().deserializer(),
                topic.getValueSerde().deserializer());

        List<TestRecord<?, ?>> records = readRecords.computeIfAbsent(topic.toString(), name -> new ArrayList<>());
        records.addAll(outputTopic.readRecordsToList());

        return List.copyOf((List<TestRecord<K, V>>) (List<?>) records);
    }

    /**
     * Format the names of the state stores of the topology for diagnostics.
     *
     * @return The formatted state store names
     */
    private String formatStateStoreNames() {
        Map<String, ?> stateStores = testDriver.getAllStateStores();
        if (stateStores.isEmpty()) {
            return "  <none>";
        }
        return format(new ArrayList<>(new TreeSet<>(stateStores.keySet())));
    }

    /**
     * Format a list of elements, one indented element per line, for diagnostics.
     *
     * @param elements The elements to format
     * @return The formatted elements
     */
    static String format(List<String> elements) {
        if (elements.isEmpty()) {
            return "  <none>";
        }
        return elements.stream().map(element -> "  " + element).collect(Collectors.joining(System.lineSeparator()));
    }

    /**
     * Format a possibly null object for diagnostics.
     *
     * @param value The value to format
     * @return The formatted value
     */
    static String display(Object value) {
        if (value == null) {
            return "<null>";
        }
        return value.toString();
    }
}
