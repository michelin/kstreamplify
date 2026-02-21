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

import com.michelin.kstreamplify.WithValue;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.hamcrest.core.IsEqual;

/**
 * A Hamcrest matcher that checks if a {@link WithValue} object has a value matching a specific value or matcher.
 *
 * @param <V> The type of the value
 *     <p><b>Example:</b>
 *     <pre>{@code
 * // Assert that a record has a specific value
 * KafkaRecord<String, String> record = new KafkaRecord<>(headers, "key", "HelloWorld");
 *
 * MatcherAssert.assertThat(
 *     record,
 *     new HasValue<>("HelloWorld")
 * );
 *
 * // Use with HasRecord matcher to validate complete record structure
 * MatcherAssert.assertThat(
 *     record,
 *     new HasRecord<>(
 *         new HasKey<>("key"),
 *         new HasValue<>("HelloWorld")
 *     )
 * );
 * }</pre>
 */
public final class HasValue<V> extends TypeSafeMatcher<WithValue<V>> {
    /** The matcher for the value of the object. */
    private final Matcher<V> expected;

    /**
     * Constructs a HasValue matcher with the given expected value matcher.
     *
     * @param expected The matcher for the value of the object
     */
    public HasValue(final Matcher<V> expected) {
        this.expected = expected;
    }

    /**
     * Constructs a HasValue matcher with the given expected value.
     *
     * @param expected The expected value of the object
     */
    public HasValue(final V expected) {
        this(new IsEqual<>(expected));
    }

    @Override
    protected boolean matchesSafely(final WithValue<V> actual) {
        return this.expected.matches(actual.value());
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("Value matching ");
        this.expected.describeTo(description);
    }
}
