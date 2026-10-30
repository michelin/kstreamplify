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

import com.michelin.kstreamplify.serde.TopicWithSerde;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

/**
 * Fluent stage feeding records to a single input topic. Records are piped to the underlying {@link TopologyTestDriver}
 * as they are declared.
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public final class GivenStage<K, V> {
    private final KstreamplifyTestContext context;
    private final TestInputTopic<K, V> inputTopic;

    /**
     * Constructor.
     *
     * @param context The parent test context
     * @param topic The input topic fed by this stage
     */
    GivenStage(KstreamplifyTestContext context, TopicWithSerde<K, V> topic) {
        this.context = context;
        this.inputTopic = context.inputTopic(topic);
    }

    /**
     * Pipe a record using the current event time.
     *
     * @param key The record key
     * @param value The record value
     * @return This stage for chaining
     */
    public GivenStage<K, V> record(K key, V value) {
        inputTopic.pipeInput(key, value, context.currentTime());
        return this;
    }

    /**
     * Pipe a record using the provided timestamp. The provided timestamp becomes the current event time, so the records
     * piped afterward without an explicit timestamp use it.
     *
     * @param key The record key
     * @param value The record value
     * @param timestamp The event time of the record
     * @return This stage for chaining
     */
    public GivenStage<K, V> record(K key, V value, Instant timestamp) {
        inputTopic.pipeInput(key, value, timestamp);
        context.currentTime(timestamp);
        return this;
    }

    /**
     * Pipe a bulk collection of records using the current event time.
     *
     * @param records The records to pipe
     * @return This stage for chaining
     */
    public GivenStage<K, V> records(Collection<KeyValue<K, V>> records) {
        records.forEach(record -> inputTopic.pipeInput(record.key, record.value, context.currentTime()));
        return this;
    }

    /**
     * Switch to another input topic while staying in the {@code given} stage.
     *
     * @param topic The next input topic to feed
     * @param <K2> The type of the next key
     * @param <V2> The type of the next value
     * @return A new {@link GivenStage} bound to the provided topic
     */
    public <K2, V2> GivenStage<K2, V2> and(TopicWithSerde<K2, V2> topic) {
        return context.given(topic);
    }

    /**
     * Advance the event time used by the records piped afterward without an explicit timestamp. As defined by Kafka
     * Streams, it does not advance the wall clock time of the topology test driver.
     *
     * @param duration The duration to advance the event time by
     * @return This stage for chaining
     */
    public GivenStage<K, V> advanceTime(Duration duration) {
        context.advanceTime(duration);
        return this;
    }

    /**
     * Advance the wall clock time of the topology test driver to trigger the wall-clock-time punctuators. As defined by
     * Kafka Streams, it does not advance the event time used by the records piped afterward.
     *
     * @param duration The duration to advance the wall clock time by
     * @return This stage for chaining
     */
    public GivenStage<K, V> advanceWallClockTime(Duration duration) {
        context.advanceWallClockTime(duration);
        return this;
    }

    /**
     * Mark the point at which the supplied inputs have been processed by the topology.
     *
     * @return A {@link WhenStage} to chain assertions
     */
    public WhenStage when() {
        return context.when();
    }

    /**
     * Start typed assertions on the records produced on the provided output topic.
     *
     * @param topic The output topic to assert on
     * @param <OK> The type of the output key
     * @param <OV> The type of the output value
     * @return An {@link OutputAssertion} holding the produced records
     */
    public <OK, OV> OutputAssertion<OK, OV> then(TopicWithSerde<OK, OV> topic) {
        return context.then(topic);
    }

    /**
     * Start assertions on the records sent to the dead letter queue.
     *
     * @return A {@link DlqAssertion} holding the DLQ records
     */
    public DlqAssertion thenDlq() {
        return context.thenDlq();
    }

    /**
     * Start assertions on the content of the provided key-value state store.
     *
     * @param storeName The name of the state store
     * @param <SK> The type of the store key
     * @param <SV> The type of the store value
     * @return A {@link StateStoreAssertion} bound to the state store
     */
    public <SK, SV> StateStoreAssertion<SK, SV> thenStateStore(String storeName) {
        return context.thenStateStore(storeName);
    }

    /**
     * Expose the underlying topology test driver as an escape hatch for advanced tests.
     *
     * @return The underlying topology test driver
     */
    public TopologyTestDriver driver() {
        return context.driver();
    }
}
