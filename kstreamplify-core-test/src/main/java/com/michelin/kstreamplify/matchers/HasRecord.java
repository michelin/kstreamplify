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

import com.michelin.kstreamplify.KafkaRecord;
import com.michelin.kstreamplify.WithHeaders;
import com.michelin.kstreamplify.WithKey;
import com.michelin.kstreamplify.WithValue;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.test.TestRecord;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * A Hamcrest matcher that checks if a {@link KafkaRecord} has specific headers, key and value.
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 *     <p><b>Example:</b>
 *     <pre>{@code
 * // Assert that a KafkaRecord has specific key and value
 * KafkaRecord<String, String> record = new KafkaRecord<>(headers, "user-1", "Hello World");
 * MatcherAssert.assertThat(
 *     record,
 *     new HasRecord<>("user-1", "Hello World")
 * );
 * }</pre>
 */
public final class HasRecord<K, V> extends TypeSafeMatcher<KafkaRecord<K, V>> {

    /** The matcher for the headers of the record. */
    private final Matcher<WithHeaders> expectedHeaders;

    /** The matcher for the key of the record. */
    private final Matcher<WithKey<K>> expectedKey;

    /** The matcher for the value of the record. */
    private final Matcher<WithValue<V>> expectedValue;

    /**
     * Constructs a HasRecord matcher with the given expected headers, key and value matchers.
     *
     * @param expectedHeaders The matcher for the headers of the record
     * @param expectedKey The matcher for the key of the record
     * @param expectedValue The matcher for the value of the record
     */
    public HasRecord(
            final Matcher<WithHeaders> expectedHeaders,
            final Matcher<WithKey<K>> expectedKey,
            final Matcher<WithValue<V>> expectedValue) {
        this.expectedHeaders = expectedHeaders;
        this.expectedKey = expectedKey;
        this.expectedValue = expectedValue;
    }

    /**
     * Constructs a HasRecord matcher with the given expected key and value matchers, and ignoring headers.
     *
     * @param expectedKey The matcher for the key of the record
     * @param expectedValue The matcher for the value of the record
     */
    public HasRecord(final K expectedKey, final V expectedValue) {
        this(new IgnoreHeaders(), new HasKey<>(expectedKey), new HasValue<>(expectedValue));
    }

    /**
     * Constructs a HasRecord matcher with the given expected key and value, and ignoring headers.
     *
     * @param kv The expected key and value of the record
     */
    public HasRecord(final KeyValue<K, V> kv) {
        this(kv.key, kv.value);
    }

    /**
     * Constructs a HasRecord matcher with the given expected key and value, and ignoring headers.
     *
     * @param entry The expected key and value of the record
     */
    public HasRecord(final Map.Entry<K, V> entry) {
        this(entry.getKey(), entry.getValue());
    }

    @Override
    protected boolean matchesSafely(final KafkaRecord<K, V> actual) {
        return Stream.of(this.expectedHeaders, this.expectedKey, this.expectedValue)
                .allMatch(matcher -> matcher.matches(actual));
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("Record matching: [");
        this.expectedHeaders.describeTo(description);
        description.appendText("], [");
        this.expectedKey.describeTo(description);
        description.appendText("], [");
        this.expectedValue.describeTo(description);
        description.appendText("]");
    }

    /**
     * A Hamcrest matcher that checks if a {@link TestRecord} has specific headers, key and value, by converting it to a
     * {@link KafkaRecord}.
     *
     * @param <K> The type of the key
     * @param <V> The type of the value
     */
    public static final class FromTestRecord<K, V> extends TypeSafeMatcher<TestRecord<K, V>> {
        /** The delegate matcher for the KafkaRecord converted from the TestRecord. */
        private final Matcher<KafkaRecord<K, V>> delegate;

        /**
         * Constructs a FromTestRecord matcher with the given delegate matcher.
         *
         * @param delegate The delegate matcher for the KafkaRecord converted from the TestRecord
         */
        public FromTestRecord(final Matcher<KafkaRecord<K, V>> delegate) {
            this.delegate = delegate;
        }

        @Override
        protected boolean matchesSafely(final TestRecord<K, V> testRecord) {
            return this.delegate.matches(new KafkaRecord<>(testRecord));
        }

        @Override
        public void describeTo(final Description description) {
            this.delegate.describeTo(description);
        }
    }
}
