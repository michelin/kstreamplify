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

import com.michelin.kstreamplify.WithKey;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.hamcrest.core.IsEqual;

/**
 * A Hamcrest matcher that checks if a {@link WithKey} object has a key matching a specific value or matcher.
 *
 * @param <K> The type of the key
 *     <p><b>Example:</b>
 *     <pre>{@code
 * // Assert that a record has a specific key
 * KafkaRecord<String, String> record = new KafkaRecord<>(headers, "user-123", "data");
 *
 * MatcherAssert.assertThat(
 *     record,
 *     new HasKey<>("user-123")
 * );
 *
 * // Use with HasRecord matcher to check multiple fields
 * MatcherAssert.assertThat(
 *     record,
 *     new HasRecord<>(
 *         new HasKey<>("user-123"),
 *         new HasValue<>("data")
 *     )
 * );
 * }</pre>
 */
public final class HasKey<K> extends TypeSafeMatcher<WithKey<K>> {
    /** The matcher for the key of the object. */
    private final Matcher<K> expected;

    /**
     * Constructs a HasKey matcher with the given expected key matcher.
     *
     * @param expected The matcher for the key of the object
     */
    public HasKey(final Matcher<K> expected) {
        this.expected = expected;
    }

    /**
     * Constructs a HasKey matcher with the given expected key.
     *
     * @param expected The expected key of the object
     */
    public HasKey(final K expected) {
        this(new IsEqual<>(expected));
    }

    @Override
    protected boolean matchesSafely(final WithKey<K> actual) {
        return this.expected.matches(actual.key());
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("Key matching ");
        this.expected.describeTo(description);
    }
}
