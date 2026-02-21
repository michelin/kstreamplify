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
package com.michelin.kstreamplify.matchers;

import static java.util.concurrent.TimeUnit.MINUTES;

import com.michelin.kstreamplify.KafkaRecord;
import com.michelin.kstreamplify.topics.PolledRecords;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.awaitility.core.DurationFactory;
import org.awaitility.pollinterval.FixedPollInterval;
import org.awaitility.pollinterval.PollInterval;
import org.cactoos.list.ListOf;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.hamcrest.collection.IsIterableContainingInOrder;

/**
 * Matcher that checks if a consumer polls the expected records from the given topics.
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 *     <p><b>Example:</b>
 *     <pre>{@code
 * // Assert that a Kafka consumer receives expected word counts
 * try (Consumer<String, Long> consumer = new KafkaConsumer<>(config)) {
 *     MatcherAssert.assertThat(
 *         consumer,
 *         new ConsumerPolls<>(
 *             Collections.singletonList("output-topic"),
 *             new MapEntry<>("hello", 1L),
 *             new MapEntry<>("kafka", 2L),
 *             new MapEntry<>("streams", 1L)
 *         )
 *     );
 * }
 * }</pre>
 */
public final class ConsumerPolls<K, V> extends TypeSafeMatcher<Consumer<K, V>> {

    /** The topics to subscribe to. */
    private final List<String> topics;

    /** The expected records to be polled. */
    private final Matcher<Iterable<? extends KafkaRecord<K, V>>> expected;

    /** The maximum duration to wait for the expected records to be polled. */
    private final Duration atMost;

    /** The interval between polls. */
    private final PollInterval pollInterval;

    /** The expected number of records to be polled. */
    private final int expectedSize;

    /** The records that were actually polled. */
    private List<KafkaRecord<K, V>> polledRecords;

    /**
     * Primary ctor.
     *
     * @param topics The topics to subscribe to
     * @param expected The expected records to be polled
     * @param atMost The maximum duration to wait for the expected records to be polled
     * @param pollInterval The interval between polls
     * @param size The expected number of records to be polled
     */
    public ConsumerPolls(
            final List<String> topics,
            final Matcher<Iterable<? extends KafkaRecord<K, V>>> expected,
            final Duration atMost,
            final PollInterval pollInterval,
            final int size) {
        this.topics = topics;
        this.expected = expected;
        this.atMost = atMost;
        this.pollInterval = pollInterval;
        this.expectedSize = size;
    }

    /**
     * Secondary ctor, for convenience.
     *
     * @param topics The topics to subscribe to
     * @param atMost The maximum duration to wait for the expected records to be polled
     * @param pollInterval The interval between polls
     * @param expectedEntries The expected records to be polled
     */
    public ConsumerPolls(
            final List<String> topics,
            final Duration atMost,
            final PollInterval pollInterval,
            final List<Matcher<KafkaRecord<K, V>>> expectedEntries) {
        this(
                topics,
                IsIterableContainingInOrder.contains(expectedEntries.toArray(new Matcher[expectedEntries.size()])),
                atMost,
                pollInterval,
                expectedEntries.size());
    }

    /**
     * Secondary ctor, for convenience.
     *
     * @param topics The topics to subscribe to
     * @param expectedEntries The expected records to be polled
     */
    @SafeVarargs
    public ConsumerPolls(
            final List<String> topics,
            final Duration atMost,
            final PollInterval pollInterval,
            final Map.Entry<K, V>... expectedEntries) {
        this(
                topics,
                atMost,
                pollInterval,
                new ListOf<>(Arrays.stream(expectedEntries).map(HasRecord::new).toList()));
    }

    /**
     * Secondary ctor, for convenience.
     *
     * @param topics The topics to subscribe to
     * @param expectedEntries The expected records to be polled
     */
    @SafeVarargs
    public ConsumerPolls(final List<String> topics, final Map.Entry<K, V>... expectedEntries) {
        this(
                topics,
                DurationFactory.of(1, MINUTES),
                new FixedPollInterval(DurationFactory.of(200, TimeUnit.MILLISECONDS)),
                expectedEntries);
    }

    @Override
    protected boolean matchesSafely(final Consumer<K, V> consumer) {
        consumer.subscribe(this.topics);
        this.polledRecords = new PolledRecords<>(consumer, this.atMost, this.pollInterval, this.expectedSize);
        return this.expected.matches(polledRecords);
    }

    @Override
    public void describeTo(final Description description) {
        this.expected.describeTo(description);
    }

    @Override
    protected void describeMismatchSafely(final Consumer<K, V> consumer, final Description description) {
        description.appendText("was ").appendValueList("[", ", ", "]", polledRecords);
    }
}
