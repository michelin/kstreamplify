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

import java.util.List;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.test.TestRecord;
import org.cactoos.list.ListOf;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.hamcrest.collection.IsIterableContainingInOrder;

/**
 * A Hamcrest matcher that checks if a {@link TestOutputTopic} contains specific records in order.
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 *     <p><b>Example:</b>
 *     <pre>{@code
 * // Assert that an output topic contains expected word counts
 * TopologyTestDriver driver = new TopologyTestDriver(topology, config);
 * TestOutputTopic<String, Long> outputTopic = driver.createOutputTopic(...);
 *
 * MatcherAssert.assertThat(
 *     outputTopic,
 *     new OutputTopicContains<>(
 *         new KeyValue<>("hello", 1L),
 *         new KeyValue<>("kafka", 2L),
 *         new KeyValue<>("streams", 1L)
 *     )
 * );
 * }</pre>
 */
public final class OutputTopicContains<K, V> extends TypeSafeMatcher<TestOutputTopic<K, V>> {

    /** The matcher for the records in the output topic. */
    private final Matcher<? super Iterable<TestRecord<K, V>>> expected;

    /** The actual records read from the output topic during the match. */
    private List<TestRecord<K, V>> actual;

    /**
     * Constructs an OutputTopicContains matcher with the given expected records matcher.
     *
     * @param expected The matcher for the records in the output topic
     */
    public OutputTopicContains(final Matcher<? super Iterable<TestRecord<K, V>>> expected) {
        this.expected = expected;
    }

    /**
     * Constructs an OutputTopicContains matcher with the given expected records.
     *
     * @param expected The expected records in the output topic
     */
    @SuppressWarnings({"unchecked"})
    public OutputTopicContains(final List<KeyValue<K, V>> expected) {
        this(IsIterableContainingInOrder.contains(expected.stream()
                .map((final KeyValue<K, V> kv) -> new HasRecord.FromTestRecord<>(new HasRecord<>(kv)))
                .toArray(Matcher[]::new)));
    }

    /**
     * Constructs an OutputTopicContains matcher with the given expected records.
     *
     * @param expected The expected records in the output topic
     */
    @SafeVarargs
    public OutputTopicContains(final KeyValue<K, V>... expected) {
        this(new ListOf<>(expected));
    }

    @Override
    protected boolean matchesSafely(final TestOutputTopic<K, V> testOutputTopic) {
        this.actual = testOutputTopic.readRecordsToList();
        return this.expected.matches(this.actual);
    }

    @Override
    public void describeTo(final Description description) {
        this.expected.describeTo(description);
    }

    @Override
    protected void describeMismatchSafely(final TestOutputTopic<K, V> item, final Description description) {
        description.appendText("was ").appendValue(this.actual);
    }
}
