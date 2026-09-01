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
import org.apache.kafka.streams.TopologyTestDriver;

/**
 * Base class of all the {@code then} stages of the testing DSL. It keeps a reference to the parent
 * {@link KstreamplifyTestContext} so that a single fluent chain can assert on several output topics, on the dead letter
 * queue and on state stores, or feed additional records.
 */
public abstract class AssertionStage {
    private final KstreamplifyTestContext context;

    /**
     * Constructor.
     *
     * @param context The parent test context
     */
    AssertionStage(KstreamplifyTestContext context) {
        this.context = context;
    }

    /**
     * Continue the chain with typed assertions on another output topic.
     *
     * @param topic The output topic to assert on
     * @param <K> The type of the key
     * @param <V> The type of the value
     * @return An {@link OutputAssertion} holding the records produced on the provided topic
     */
    public <K, V> OutputAssertion<K, V> and(TopicWithSerde<K, V> topic) {
        return context.then(topic);
    }

    /**
     * Continue the chain with assertions on the records sent to the dead letter queue.
     *
     * @return A {@link DlqAssertion} holding the DLQ records
     */
    public DlqAssertion andDlq() {
        return context.thenDlq();
    }

    /**
     * Continue the chain with assertions on the content of the provided key-value state store.
     *
     * @param storeName The name of the state store
     * @param <K> The type of the key
     * @param <V> The type of the value
     * @return A {@link StateStoreAssertion} bound to the state store
     */
    public <K, V> StateStoreAssertion<K, V> andStateStore(String storeName) {
        return context.thenStateStore(storeName);
    }

    /**
     * Continue the chain by feeding additional records to an input topic.
     *
     * @param topic The input topic to feed
     * @param <K> The type of the key
     * @param <V> The type of the value
     * @return A {@link GivenStage} bound to the provided topic
     */
    public <K, V> GivenStage<K, V> andGiven(TopicWithSerde<K, V> topic) {
        return context.given(topic);
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
