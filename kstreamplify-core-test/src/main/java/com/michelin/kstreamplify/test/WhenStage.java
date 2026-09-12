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
import org.apache.kafka.streams.TopologyTestDriver;

/**
 * Fluent stage representing the point at which the supplied inputs have been processed by the topology. It is the entry
 * point for output, DLQ and state store assertions.
 */
public final class WhenStage {
    private final KstreamplifyTestContext context;

    /**
     * Constructor.
     *
     * @param context The parent test context
     */
    WhenStage(KstreamplifyTestContext context) {
        this.context = context;
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
     * @param <K> The type of the key
     * @param <V> The type of the value
     * @return A {@link StateStoreAssertion} bound to the state store
     */
    public <K, V> StateStoreAssertion<K, V> thenStateStore(String storeName) {
        return context.thenStateStore(storeName);
    }

    /**
     * Advance the event time used by the records piped afterward without an explicit timestamp. As defined by Kafka
     * Streams, it does not advance the wall clock time of the topology test driver.
     *
     * @param duration The duration to advance the event time by
     * @return This stage for chaining
     */
    public WhenStage advanceTime(Duration duration) {
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
    public WhenStage advanceWallClockTime(Duration duration) {
        context.advanceWallClockTime(duration);
        return this;
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
