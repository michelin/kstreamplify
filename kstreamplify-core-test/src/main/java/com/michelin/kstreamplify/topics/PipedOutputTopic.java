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
package com.michelin.kstreamplify.topics;

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.cactoos.BiFunc;
import org.cactoos.Scalar;
import org.cactoos.scalar.Unchecked;

/**
 * A {@link BiFunc} that pipes input to a {@link TestInputTopic} and returns a {@link TestOutputTopic}.
 *
 * @param <X> The type of the key of the input topic
 * @param <Z> The type of the value of the input topic
 * @param <K> The type of the key of the output topic
 * @param <V> The type of the value of the output topic
 *     <p><b>Example:</b>
 *     <pre>{@code
 * // Pipe input to a topology and get output topic
 * TopologyTestDriver driver = new TopologyTestDriver(topology, config);
 * TestOutputTopic<String, Long> result = new PipedOutputTopic<>(
 *     new WordCountInputTopic(driver),
 *     new WordCountOutputTopic(driver)
 * ).apply("key1", "Hello Kafka Kafka Streams");
 *
 * MatcherAssert.assertThat(
 *     result,
 *     new OutputTopicContains<>(
 *         new KeyValue<>("hello", 1L),
 *         new KeyValue<>("kafka", 2L),
 *         new KeyValue<>("streams", 1L)
 *     )
 * );
 * }</pre>
 */
public final class PipedOutputTopic<X, Z, K, V> implements BiFunc<X, Z, TestOutputTopic<K, V>> {

    /** The input topic to which the key and value will be piped. */
    private final TestInputTopic<X, Z> inputTopic;

    /** The output topic that will be returned after piping the input. */
    private final TestOutputTopic<K, V> outputTopic;

    /**
     * Primary ctor.
     *
     * @param inputTopic The input topic to which the key and value will be piped
     * @param outputTopic The output topic that will be returned after piping the input
     */
    public PipedOutputTopic(final TestInputTopic<X, Z> inputTopic, final TestOutputTopic<K, V> outputTopic) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    /**
     * Ctor that accepts scalars for the input and output topics.
     *
     * @param inputTopic The scalar for the input topic to which the key and value will be piped
     * @param outputTopic The scalar for the output topic that will be returned after piping the input
     */
    public PipedOutputTopic(
            final Scalar<TestInputTopic<X, Z>> inputTopic, final Scalar<TestOutputTopic<K, V>> outputTopic) {
        this(new Unchecked<>(inputTopic).value(), new Unchecked<>(outputTopic).value());
    }

    @Override
    public TestOutputTopic<K, V> apply(final X key, final Z value) {
        this.inputTopic.pipeInput(key, value);
        return this.outputTopic;
    }
}
